/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.elasticsearch.{scaladsl, _}
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

/**
 * Java API to create Elasticsearch flows.
 */
object ElasticsearchFlow {

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param objectMapper Jackson object mapper converting type `T` to JSON
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    create(indexName, typeName, settings, elasticsearchClient, new JacksonWriter[T](objectMapper))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param messageWriter converts type `T` to a `String` containing valid JSON
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    scaladsl.ElasticsearchFlow
      .create(indexName, typeName, settings, messageWriter)(elasticsearchClient)
      .asJava

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param objectMapper Jackson object mapper converting type `T` to JSON
   */
  def createWithPassThrough[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    createWithPassThrough(indexName, typeName, settings, elasticsearchClient, new JacksonWriter[T](objectMapper))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param messageWriter converts type `T` to a `String` containing valid JSON
   */
  def createWithPassThrough[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    scaladsl.ElasticsearchFlow
      .createWithPassThrough(indexName, typeName, settings, messageWriter)(elasticsearchClient)
      .asJava

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * @param objectMapper Jackson object mapper converting type `T` to JSON
   * @throws IllegalArgumentException When settings configure retrying.
   */
  @ApiMayChange
  def createWithContext[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    createWithContext(indexName, typeName, settings, elasticsearchClient, new JacksonWriter[T](objectMapper))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * @param messageWriter converts type `T` to a `String` containing valid JSON
   * @throws IllegalArgumentException When settings configure retrying.
   */
  @ApiMayChange
  def createWithContext[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    scaladsl.ElasticsearchFlow
      .createWithContext(indexName, typeName, settings, messageWriter)(elasticsearchClient)
      .asJava

  private final class JacksonWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
