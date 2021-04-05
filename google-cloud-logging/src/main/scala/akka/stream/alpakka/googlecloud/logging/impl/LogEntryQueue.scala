/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.OverflowStrategies.{DropHead, DropTail}
import akka.stream._
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity}
import akka.stream.scaladsl.Flow
import akka.stream.stage._

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.Ordering.Implicits._

@InternalApi
private[logging] object LogEntryQueue {

  def apply[T](queueSize: Int,
               flushSeverity: LogSeverity,
               flushWithin: FiniteDuration,
               overflowStrategy: OverflowStrategy): Flow[LogEntry[T], immutable.Seq[LogEntry[T]], NotUsed] =
    Flow.fromGraph(new LogEntryQueue[T](queueSize, flushSeverity, flushWithin, overflowStrategy))

  case object LogEntryQueueTimerKey
}

/**
 * ==Properties of this stage==
 *  1. The stage never backpressures.
 *  1. The queue never contains more than `queueSize` entries.
 *  1. If the queue exceeds `queueSize` and the downstream is backpressured, the lowest severity entry will be dropped.
 *     [[OverflowStrategy.dropHead]] will drop older entries and [[OverflowStrategy.dropTail]] will drop younger entries.
 *  1. The following conditions will cause all entries in the queue to be emitted as soon as the downstream becomes available.
 *     Furthermore, these are the only conditions in which the stage will emit.
 *       i. the queue is full (i.e., contains at least `queueSize` entries)
 *       i. the queue contains an entry of severity at least `flushSeverity`
 *       i. the queue has been non-empty for at least `flushWithin`
 *
 * ==Reactive Streams semantics:==
 *
 *  - <b>emits</b> when downstream stops backpressuring, the queue is non-empty, and either `flushWithin` has elapsed
 *    since the queue became non-empty or the queue is full or contains at least one entry of severity greater than or
 *    equal to `flushSeverity`.
 *
 *  - <b>backpressures</b> never, either by emitting or, if downstream is backpressured, by dropping the oldest
 *    ([[OverflowStrategy.dropHead]]) or youngest ([[OverflowStrategy.dropTail]]) entry with the lowest severity that is
 *    currently in the queue.
 *
 *  - <b>completes</b> when upstream completes and queue is drained.
 */
@InternalApi
private[impl] final class LogEntryQueue[T] private (queueSize: Int,
                                                    flushSeverity: LogSeverity,
                                                    flushWithin: FiniteDuration,
                                                    overflowStrategy: OverflowStrategy)
    extends GraphStage[FlowShape[LogEntry[T], immutable.Seq[LogEntry[T]]]] {
  require(queueSize > 0, "queueSize must be greater than 0")

  implicit private val ordering: Ordering[LogEntry[T]] = overflowStrategy match {
    case DropHead(_) => LogEntry.ordering(true).reverse // Reverse so that deque() drops lowest priority
    case DropTail(_) => LogEntry.ordering(false).reverse
    case _ => throw new IllegalArgumentException(s"Only DropHead and DropTail strategies supported")
  }

  private val in = Inlet[LogEntry[T]]("in")
  private val out = Outlet[immutable.Seq[LogEntry[T]]]("out")

  override def initialAttributes = Attributes.name("logEntryQueue")

  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private val queue = new mutable.PriorityQueue[LogEntry[T]]()
      // True if:
      // - queue is nonEmpty
      //       AND
      // - (timer fired
      //        OR
      //    queue.size >= queueSize
      //        OR
      //    queue.exists(_.severity >= flushSeverity)
      //        OR
      //    upstream completed)
      private var pushEagerly = false
      private var finished = false

      override def preStart(): Unit = {
        pull(in)
      }

      private def nextElement(elem: LogEntry[T]): Unit = {
        if (queue.isEmpty) {
          if (flushWithin > Duration.Zero)
            scheduleWithFixedDelay(LogEntryQueue.LogEntryQueueTimerKey, flushWithin, flushWithin)
          else
            pushEagerly = true
        }

        queue += elem

        pushEagerly ||= elem.severity.exists(_ >= flushSeverity) || queue.size >= queueSize

        if (queue.size > queueSize && !isAvailable(out))
          queue.dequeue()

        if (pushEagerly && isAvailable(out))
          emitGroup()

        pull(in)
      }

      private def tryCloseGroup(): Unit = {
        if (isAvailable(out)) emitGroup()
        else if (finished) pushEagerly = true
      }

      private def emitGroup(): Unit = {
        push(out, queue.toVector)
        queue.clear()
        if (!finished) startNewGroup()
        else completeStage()
      }

      private def startNewGroup(): Unit = {
        pushEagerly = false
      }

      override def onPush(): Unit = {
        nextElement(grab(in))
      }

      override def onPull(): Unit = if (pushEagerly) emitGroup()

      override def onUpstreamFinish(): Unit = {
        finished = true
        if (queue.isEmpty) completeStage()
        else tryCloseGroup()
      }

      override protected def onTimer(timerKey: Any): Unit = if (queue.nonEmpty) {
        if (isAvailable(out)) emitGroup()
        else pushEagerly = true
      }

      setHandlers(in, out, this)
    }
}
