/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.internal.stage

import akka.Done
import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import org.apache.geode.cache.client.ClientCache

import scala.concurrent.{Future, Promise}

class GeodeFiniteSourceStage[V](cache: ClientCache, sql: String)
    extends GraphStageWithMaterializedValue[SourceShape[V], Future[Done]] {

  override protected def initialAttributes: Attributes =
    Attributes.name("GeodeFiniteSource").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  val out = Outlet[V]("geode.finiteSource")

  override def shape: SourceShape[V] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subPromise = Promise[Done]

    (new GeodeQueryGraphLogic[V](shape, cache, sql) {

      override val onConnect: AsyncCallback[Unit] = getAsyncCallback[Unit] { v =>
        subPromise.success(Done)
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull() =
            if (initialResultsIterator.hasNext)
              push(out, initialResultsIterator.next())
            else
              completeStage()
        }
      )

    }, subPromise.future)
  }
}
