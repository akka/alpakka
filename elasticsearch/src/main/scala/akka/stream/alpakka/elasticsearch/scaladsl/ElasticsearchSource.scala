/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchSourceSettings, ElasticsearchSourceStage, OutgoingMessage}
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestClient
import spray.json.{DefaultJsonProtocol, JsObject, JsonReader}

object ElasticsearchSource {

  /**
   * Scala API: creates a [[ElasticsearchSourceStage]] that consumes as JsObject
   */
  def apply(indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient
  ): Source[OutgoingMessage[JsObject], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(indexName, typeName, query, client, settings)(
        DefaultJsonProtocol.RootJsObjectFormat
      )
    )

  /**
   * Scala API: creates a [[ElasticsearchSourceStage]] that consumes as specific type
   */
  def typed[T](indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient,
      reader: JsonReader[T]
  ): Source[OutgoingMessage[T], NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, settings))

}
