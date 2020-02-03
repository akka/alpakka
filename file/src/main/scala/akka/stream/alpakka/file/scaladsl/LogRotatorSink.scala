/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.{OpenOption, Path, StandardOpenOption}

import akka.Done
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.impl.fusing.MapAsync.{Holder, NotYetThere}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Scala API.
 */
object LogRotatorSink {

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `triggerGenerator` returns a value.
   *
   * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
   * @param fileOpenOptions file options for file creation
   */
  def apply(
      triggerGeneratorCreator: () => ByteString => Option[Path],
      fileOpenOptions: Set[OpenOption] = Set(StandardOpenOption.APPEND, StandardOpenOption.CREATE)
  ): Sink[ByteString, Future[Done]] =
    Sink.fromGraph(
      new LogRotatorSink[Path, IOResult](triggerGeneratorCreator, sinkFactory = FileIO.toPath(_, fileOpenOptions))
    )

  /**
   * Sink directing the incoming `ByteString`s to a new `Sink` created by `sinkFactory` whenever `triggerGenerator` returns a value.
   *
   * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
   * @param sinkFactory creates sinks for `ByteString`s from the value returned by `triggerGenerator`
   * @tparam C criterion type (for files a `Path`)
   * @tparam R result type in materialized futures of `sinkFactory`
   **/
  def withSinkFactory[C, R](
      triggerGeneratorCreator: () => ByteString => Option[C],
      sinkFactory: C => Sink[ByteString, Future[R]]
  ): Sink[ByteString, Future[Done]] =
    Sink.fromGraph(new LogRotatorSink[C, R](triggerGeneratorCreator, sinkFactory))
}

/**
 * "Log Rotator Sink" graph stage
 *
 * @param triggerGeneratorCreator creates a function that triggers rotation by returning a value
 * @param sinkFactory creates sinks for `ByteString`s from the value returned by `triggerGenerator`
 * @tparam C criterion type (for files a `Path`)
 * @tparam R result type in materialized futures of `sinkFactory`
 */
final private class LogRotatorSink[C, R](triggerGeneratorCreator: () => ByteString => Option[C],
                                         sinkFactory: C => Sink[ByteString, Future[R]])
    extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Done]] {

  val in = Inlet[ByteString]("LogRotatorSink.in")
  override val shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val decider =
      inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    val logic = new Logic(promise, decider)
    (logic, promise.future)
  }

  private final class Logic(promise: Promise[Done], decider: Decider) extends GraphStageLogic(shape) {
    val triggerGenerator: ByteString => Option[C] = triggerGeneratorCreator()
    var sourceOut: SubSourceOutlet[ByteString] = _
    var sinkCompletions: immutable.Seq[Future[R]] = immutable.Seq.empty

    def failThisStage(ex: Throwable): Unit =
      if (!promise.isCompleted) {
        if (sourceOut != null) {
          sourceOut.fail(ex)
        }
        cancel(in)
        promise.failure(ex)
      }

    def checkTrigger(data: ByteString): Option[C] =
      try {
        triggerGenerator(data)
      } catch {
        case ex: Throwable =>
          failThisStage(ex)
          None
      }

    def sinkCompletionCallbackHandler(future: Future[R])(h: Holder[R]): Unit =
      h.elem match {
        case Success(IOResult(_, Failure(ex))) if decider(ex) == Supervision.Stop =>
          promise.failure(ex)
        case Success(x) if sinkCompletions.size == 1 && sinkCompletions.head == future =>
          promise.trySuccess(Done)
          completeStage()
        case x: Success[R] =>
          sinkCompletions = sinkCompletions.filter(_ != future)
        case Failure(ex) =>
          failThisStage(ex)
      }

    //init stage where we are waiting for the first path
    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val data = grab(in)
          checkTrigger(data) match {
            case None => if (!isClosed(in)) pull(in)
            case Some(triggerValue) => rotate(triggerValue, data)
          }
        }

        override def onUpstreamFinish(): Unit = {
          promise.trySuccess(Done)
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit =
          failThisStage(ex)
      }
    )

    //we must pull the first element cos we are a sink
    override def preStart(): Unit = {
      super.preStart()
      pull(in)
    }

    def futureCB(newFuture: Future[R]) =
      getAsyncCallback[Holder[R]](sinkCompletionCallbackHandler(newFuture))

    //we recreate the tail of the stream, and emit the data for the next req
    def rotate(triggerValue: C, data: ByteString): Unit = {
      val prevOut = Option(sourceOut)

      sourceOut = new SubSourceOutlet[ByteString]("LogRotatorSink.sub-out")
      sourceOut.setHandler(new OutHandler {
        override def onPull(): Unit = {
          sourceOut.push(data)
          switchToNormalMode()
        }
      })
      val newFuture = Source
        .fromGraph(sourceOut.source)
        .runWith(sinkFactory(triggerValue))(interpreter.subFusingMaterializer)

      sinkCompletions = sinkCompletions :+ newFuture

      val holder = new Holder[R](NotYetThere, futureCB(newFuture))

      newFuture.onComplete(holder)(
        akka.dispatch.ExecutionContexts.sameThreadExecutionContext
      )

      prevOut.foreach(_.complete())
    }

    //we change path if needed or push the grabbed data
    def switchToNormalMode(): Unit = {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val data = grab(in)
            checkTrigger(data) match {
              case None => sourceOut.push(data)
              case Some(triggerValue) => rotate(triggerValue, data)
            }
          }

          override def onUpstreamFinish(): Unit = {
            implicit val executionContext: ExecutionContext =
              akka.dispatch.ExecutionContexts.sameThreadExecutionContext
            promise.completeWith(Future.sequence(sinkCompletions).map(_ => Done))
            sourceOut.complete()
          }

          override def onUpstreamFailure(ex: Throwable): Unit =
            failThisStage(ex)
        }
      )
      sourceOut.setHandler(new OutHandler {
        override def onPull(): Unit =
          pull(in)
      })
    }
  }

}
