/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl.stage

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.geode.RegionSettings
import akka.stream.alpakka.geode.impl.GeodeCapabilities
import akka.stream.stage._
import org.apache.geode.cache.client.ClientCache

@InternalApi
private[geode] class GeodeFlowStage[K, T <: AnyRef](cache: ClientCache, settings: RegionSettings[K, T])
    extends GraphStage[FlowShape[T, T]] {

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name("GeodeFlow") and ActorAttributes.IODispatcher

  private val in = Inlet[T]("geode.in")
  private val out = Outlet[T]("geode.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging with GeodeCapabilities[K, T] {

      override protected def logSource = classOf[GeodeFlowStage[K, T]]

      val regionSettings = settings

      val clientCache = cache

      setHandler(out, new OutHandler {
        override def onPull() =
          pull(in)
      })

      setHandler(in, new InHandler {
        override def onPush() = {
          val msg = grab(in)

          put(msg)

          push(out, msg)
        }

      })

      override def postStop() = {
        log.debug("Stage completed")
        close()
      }
    }

}
