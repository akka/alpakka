/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchFlowStage, IncomingMessage, MessageWriter}
import akka.stream.scaladsl.Flow
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

import scala.collection.JavaConverters._
import java.util.{List => JavaList, Map => JavaMap}

object ElasticsearchFlow {

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient
  ): akka.stream.javadsl.Flow[IncomingMessage[JavaMap[String, Object]], JavaList[
    IncomingMessage[JavaMap[String, Object]]
  ], NotUsed] =
    create(indexName, typeName, settings, client, new ObjectMapper())

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[IncomingMessage[JavaMap[String, Object]], JavaList[
    IncomingMessage[JavaMap[String, Object]]
  ], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[JavaMap[String, Object], JavaList[IncomingMessage[JavaMap[String, Object]]]](
          indexName,
          typeName,
          client,
          settings.asScala,
          _.asJava,
          new JacksonWriter[JavaMap[String, Object]](objectMapper)
        )
      )
      .mapAsync(1)(identity)
      .asJava

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts specific type
   */
  def typed[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient
  ): akka.stream.javadsl.Flow[IncomingMessage[T], JavaList[IncomingMessage[T]], NotUsed] =
    typed(indexName, typeName, settings, client, new ObjectMapper())

  /**
   * Java API: creates a [[ElasticsearchFlowStage]] that accepts specific type
   */
  def typed[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[IncomingMessage[T], JavaList[IncomingMessage[T]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, JavaList[IncomingMessage[T]]](indexName,
                                                                    typeName,
                                                                    client,
                                                                    settings.asScala,
                                                                    _.asJava,
                                                                    new JacksonWriter[T](objectMapper))
      )
      .mapAsync(1)(identity)
      .asJava

  private class JacksonWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
