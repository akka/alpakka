/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Keep, Sink}
import akka.{Done, NotUsed}
import spray.json.JsonWriter

import scala.concurrent.Future

/**
 * Scala API to create Elasticsearch sinks.
 */
object ElasticsearchSink {

  /**
   * Create a sink to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   */
  def create[T](esParams: EsParams, settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit sprayJsonWriter: JsonWriter[T]
  ): Sink[WriteMessage[T, NotUsed], Future[Done]] =
    ElasticsearchFlow.create[T](esParams, settings).toMat(Sink.ignore)(Keep.right)

}
