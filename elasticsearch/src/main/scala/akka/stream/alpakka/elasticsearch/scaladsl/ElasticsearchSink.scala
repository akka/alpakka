/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.scaladsl

import akka.Done
import akka.stream.alpakka.elasticsearch.{
  ElasticsearchSinkSettings,
  ElasticsearchSinkStage,
  ElasticsearchSinkStageTyped,
  IncomingMessage
}
import akka.stream.scaladsl.Sink
import org.elasticsearch.client.RestClient
import spray.json.{JsObject, JsonWriter}

import scala.concurrent.Future

object ElasticsearchSink {

  /**
   * Scala API: creates a [[ElasticsearchSinkStage]] that consumes as JsObject
   */
  def apply(indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient): Sink[IncomingMessage[JsObject], Future[Done]] =
    Sink.fromGraph(new ElasticsearchSinkStage(indexName, typeName, client, settings))

  /**
   * Scala API: creates a [[ElasticsearchSinkStage]] that consumes as specific type
   */
  def typed[T](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]): Sink[IncomingMessage[T], Future[Done]] =
    Sink.fromGraph(new ElasticsearchSinkStageTyped(indexName, typeName, client, settings))

}
