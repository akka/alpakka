/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchFlowStage, ElasticsearchSinkSettings, IncomingMessage}
import akka.stream.javadsl.Flow
import org.elasticsearch.client.{Response, RestClient}
import spray.json.{JsObject, JsonWriter}

object ElasticsearchFlow {

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSinkSettings,
             client: RestClient): Flow[IncomingMessage[JsObject], Response, NotUsed] =
    akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow(indexName, typeName, settings)(client).asJava

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSinkSettings,
               client: RestClient,
               writer: JsonWriter[T]): Flow[IncomingMessage[T], Response, NotUsed] =
    akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow
      .typed(indexName, typeName, settings)(client, writer)
      .asJava

}
