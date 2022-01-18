/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.elasticsearch.{scaladsl, _}
import com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters._

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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    create(elasticsearchParams, settings, new JacksonWriter[T](objectMapper))

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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    scaladsl.ElasticsearchFlow
      .create(elasticsearchParams, settings, messageWriter)
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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    createWithPassThrough(elasticsearchParams, settings, new JacksonWriter[T](objectMapper))

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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    scaladsl.ElasticsearchFlow
      .createWithPassThrough(elasticsearchParams, settings, messageWriter)
      .asJava

  /**
   * Create a flow to update Elasticsearch with
   * [[java.util.List[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[java.util.List[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]]
   * and must be checked for successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param objectMapper Jackson object mapper converting type `T` to JSON
   */
  def createBulk[T, C](
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[java.util.List[WriteMessage[T, C]], java.util.List[WriteResult[T, C]], NotUsed] =
    createBulk(elasticsearchParams, settings, new JacksonWriter[T](objectMapper))

  /**
   * Create a flow to update Elasticsearch with
   * [[java.util.List[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[java.util.List[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]]
   * and must be checked for successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param messageWriter converts type `T` to a `String` containing valid JSON
   */
  def createBulk[T, C](
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.Flow[java.util.List[WriteMessage[T, C]], java.util.List[WriteResult[T, C]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[WriteMessage[T, C]]]
      .map(_.asScala.toIndexedSeq)
      .via(
        scaladsl.ElasticsearchFlow
          .createBulk(elasticsearchParams, settings, messageWriter)
      )
      .map(_.asJava)
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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    createWithContext(elasticsearchParams, settings, new JacksonWriter[T](objectMapper))

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
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      messageWriter: MessageWriter[T]
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    scaladsl.ElasticsearchFlow
      .createWithContext(elasticsearchParams, settings, messageWriter)
      .asJava

  private final class JacksonWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
