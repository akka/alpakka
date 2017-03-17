/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{
  ElasticsearchSourceSettings,
  ElasticsearchSourceStage,
  ElasticsearchSourceStageTyped,
  OutgoingMessage
}
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestClient
import spray.json.{JsObject, JsonReader}

object ElasticsearchSource {

  /**
   * Scala API: creates a [[ElasticsearchSourceStage]] that consumes as JsObject
   */
  def apply(indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient): Source[OutgoingMessage[JsObject], NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, settings))

  /**
   * Scala API: creates a [[ElasticsearchSourceStage]] that consumes as specific type
   */
  def typed[T](indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient,
      reader: JsonReader[T]): Source[OutgoingMessage[T], NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStageTyped(indexName, typeName, query, client, settings))

}
