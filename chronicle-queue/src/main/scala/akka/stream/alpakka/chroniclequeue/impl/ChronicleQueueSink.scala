/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.chroniclequeue.impl

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.stream._
import akka.stream.stage._

import com.typesafe.config.Config

class ChronicleQueueSink[T](val queue: PersistentQueue[T], onPushCallback: () => Unit = () => {})(
    implicit serializer: ChronicleQueueSerializer[T],
    system: ActorSystem
) extends GraphStage[SinkShape[T]] {

  def this(config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](config))

  def this(persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](persistDir))

  private[stream] val in = Inlet[T]("ChronicleQueue.in")
  override val shape: SinkShape[T] = SinkShape.of(in)

  protected val queueCloserActor = system.actorOf(Props(classOf[PersistentQueueCloserActor[T]], queue))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var terminating = false

      override def preStart(): Unit = {
        super.preStart()
        pull(in)
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = {
            terminating = true
            queueCloserActor ! UpstreamFinished
            completeStage()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            terminating = true
            queueCloserActor ! UpstreamFailed
            failStage(ex)
          }

          override def onPush(): Unit =
            if (!terminating) {
              val element = grab(in)
              queue.enqueue(element)
              onPushCallback()

              pull(in)
            }
        }
      )
    }
}
