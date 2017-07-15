/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.internal.stage

import akka.Done
import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import org.apache.geode.cache.client.ClientCache

import scala.concurrent.{Future, Promise}

class GeodeContinuousSourceStage[V](cache: ClientCache, name: Symbol, sql: String)
    extends GraphStageWithMaterializedValue[SourceShape[V], Future[Done]] {

  override protected def initialAttributes: Attributes =
    Attributes
      .name("GeodeContinuousSource")
      .and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  val out = Outlet[V](s"geode.continuousSource")

  override def shape: SourceShape[V] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subPromise = Promise[Done]

    (new GeodeCQueryGraphLogic[V](shape, cache, name, sql) {

      override val onConnect: AsyncCallback[Unit] = getAsyncCallback[Unit] { v =>
        subPromise.success(Done)
      }

      val onElement: AsyncCallback[V] = getAsyncCallback[V] { element =>
        if (isAvailable(out)) {
          pushElement(out, element)
        } else
          enqueue(element)
        handleTerminaison()
      }

      //
      // This handler, will first forward initial (old) result, then new ones (continuous).
      //
      setHandler(
        out,
        new OutHandler {
          override def onPull() = {
            if (initialResultsIterator.hasNext)
              push(out, initialResultsIterator.next())
            else
              dequeue() foreach { e =>
                pushElement(out, e)
              }
            handleTerminaison()
          }
        }
      )

    }, subPromise.future)
  }
}
