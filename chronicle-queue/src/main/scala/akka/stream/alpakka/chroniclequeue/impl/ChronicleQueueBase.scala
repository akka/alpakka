/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

// ORIGINAL LICENCE
/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.stream.alpakka.chroniclequeue.impl

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, StageLogging}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.typesafe.config.Config

abstract class ChronicleQueueBase[T, S](
    private[stream] val queue: PersistentQueue[T],
    onPushCallback: () => Unit = () => {}
)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem)
    extends GraphStage[FlowShape[T, S]] {

  def this(config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](config))

  def this(persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](persistDir))

  private[stream] val in = Inlet[T]("ChronicleQueue.in")
  private[stream] val out = Outlet[S]("ChronicleQueue.out")
  val shape: FlowShape[T, S] = FlowShape.of(in, out)
  val defaultOutputPort = 0
  @volatile protected var upstreamFailed = false
  @volatile protected var upstreamFinished = false
  protected val queueCloserActor = system.actorOf(Props(classOf[PersistentQueueCloserActor[T]], queue))

  protected def elementOut(e: Event[T]): S

  protected def autoCommit(index: Long) = {}

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {

    private var lastPushed = 0L
    private var downstreamWaiting = false

    override def preStart(): Unit =
      // Start upstream demand
      pull(in)

    setHandler(
      in,
      new InHandler {

        override def onPush(): Unit = {
          val element = grab(in)
          queue.enqueue(element)
          onPushCallback()
          if (downstreamWaiting) {
            queue.dequeue().foreach { element =>
              push(out, elementOut(element))
              downstreamWaiting = false
              lastPushed = element.index
              autoCommit(element.index)
            }
          }
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          upstreamFinished = true

          if (downstreamWaiting) {
            queueCloserActor ! PushedAndCommitted(defaultOutputPort, lastPushed, queue.read(defaultOutputPort))
            queueCloserActor ! UpstreamFinished
            completeStage()
          }
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          log.error("Received upstream failure signal: " + ex)
          upstreamFailed = true
          queueCloserActor ! UpstreamFailed
          completeStage()
        }
      }
    )

    setHandler(
      out,
      new OutHandler {

        override def onPull(): Unit =
          queue.dequeue() match {
            case Some(element) =>
              push(out, elementOut(element))
              lastPushed = element.index
              autoCommit(element.index)
            case None =>
              if (upstreamFinished) {
                queueCloserActor ! PushedAndCommitted(defaultOutputPort, lastPushed, queue.read(defaultOutputPort))
                queueCloserActor ! UpstreamFinished
                completeStage()
              } else downstreamWaiting = true
          }
      }
    )
  }
}
