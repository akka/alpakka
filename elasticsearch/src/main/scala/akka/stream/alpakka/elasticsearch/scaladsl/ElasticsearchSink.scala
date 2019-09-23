/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Keep, Sink}
import org.elasticsearch.client.RestClient
import spray.json.{JsValue, JsonWriter}

import scala.concurrent.Future

/**
 * Scala API to create Elasticsearch sinks.
 */
object ElasticsearchSink {

  /**
   * Create a sink to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   */
  def create[T, P](indexName: String,
    typeName: String,
    settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
  implicit elasticsearchClient: RestClient,
  sprayJsonWriter: JsonWriter[T],
  paramJsonWrite: JsonWriter[P]
  ): Sink[WriteMessage[T, NotUsed, P], Future[Done]] =
    ElasticsearchFlow.create[T, P](indexName, typeName, settings).toMat(Sink.ignore)(Keep.right)


}
