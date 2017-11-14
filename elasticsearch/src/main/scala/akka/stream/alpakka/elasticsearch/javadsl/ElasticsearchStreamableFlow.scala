/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.{List => JavaList, Map => JavaMap}

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow.JacksonWriter
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Flow
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

import scala.collection.JavaConverters._

object ElasticsearchStreamableFlow {

  /**
   * Java API
   */
  def create[T <: StreamableIncomingMessage](
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[T, StreamableIncomingMessagesResult[T], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchStreamableFlowStage[T](indexName, typeName, client, settings.asScala)
      )
      .mapAsync(1)(identity)
      .asJava
}
