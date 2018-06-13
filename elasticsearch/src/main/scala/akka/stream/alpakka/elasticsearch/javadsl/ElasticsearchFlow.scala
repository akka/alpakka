/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Flow
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

import scala.collection.JavaConverters._
import java.util.{List => JavaList}

/**
 * Java API to create Elasticsearch flows.
 */
object ElasticsearchFlow {

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[IncomingMessage]] to lists of [[IncomingMessageResult]].
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[IncomingMessage[T, NotUsed], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, NotUsed](indexName,
                                               typeName,
                                               client,
                                               settings,
                                               new JacksonWriter[T](objectMapper))
      )
      .mapAsync(1)(identity)
      .map(x => x.asJava)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[IncomingMessage]] to lists of [[IncomingMessageResult]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[IncomingMessage[T, C], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, C](indexName, typeName, client, settings, new JacksonWriter[T](objectMapper))
      )
      .mapAsync(1)(identity)
      .map(x => x.asJava)
      .asJava

  private class JacksonWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
