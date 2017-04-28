/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchFlowStage, ElasticsearchSinkSettings, IncomingMessage}
import akka.stream.scaladsl.Flow
import org.elasticsearch.client.{Response, RestClient}
import spray.json.{DefaultJsonProtocol, JsObject, JsonWriter}

object ElasticsearchFlow {

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient
  ): Flow[IncomingMessage[JsObject], Response, NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage(indexName, typeName, client, settings)(DefaultJsonProtocol.RootJsObjectFormat)
      )
      .mapAsync(settings.parallelism)(identity)

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts specific type
   */
  def typed[T](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[IncomingMessage[T], Response, NotUsed] =
    Flow
      .fromGraph(new ElasticsearchFlowStage[T](indexName, typeName, client, settings))
      .mapAsync(settings.parallelism)(identity)

}
