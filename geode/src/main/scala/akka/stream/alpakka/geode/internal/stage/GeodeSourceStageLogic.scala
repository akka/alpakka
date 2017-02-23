/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.internal.stage

import akka.stream.SourceShape
import akka.stream.stage.{AsyncCallback, GraphStageLogic}
import org.apache.geode.cache.client.ClientCache

import scala.util.{Failure, Success, Try}

abstract class GeodeSourceStageLogic[V](shape: SourceShape[V], clientCache: ClientCache)
    extends GraphStageLogic(shape) {

  protected var initialResultsIterator: java.util.Iterator[V] = _

  val onConnect: AsyncCallback[Unit]

  lazy val qs = clientCache.getQueryService()

  def executeQuery(): Try[java.util.Iterator[V]]

  final override def preStart(): Unit = executeQuery() match {
    case Success(it) =>
      initialResultsIterator = it
      onConnect.invoke(())
    case Failure(e) =>
      failStage(e)

  }
}
