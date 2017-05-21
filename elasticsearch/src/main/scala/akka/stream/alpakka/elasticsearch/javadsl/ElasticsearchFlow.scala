/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{
  ElasticsearchFlowStage,
  ElasticsearchSinkSettings,
  IncomingMessage,
  MessageWriter
}
import akka.stream.scaladsl.Flow
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.{Response, RestClient}

object ElasticsearchFlow {

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient
  ): akka.stream.javadsl.Flow[IncomingMessage[java.util.Map[String, Object]], Response, NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage(indexName,
                                   typeName,
                                   client,
                                   settings,
                                   new JacksonWriter[java.util.Map[String, Object]]())
      )
      .mapAsync(1)(identity)
      .asJava

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSinkSettings,
               client: RestClient): akka.stream.javadsl.Flow[IncomingMessage[T], Response, NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T](indexName, typeName, client, settings, new JacksonWriter[T]())
      )
      .mapAsync(1)(identity)
      .asJava

  private class JacksonWriter[T] extends MessageWriter[T] {

    private val mapper = new ObjectMapper()

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
