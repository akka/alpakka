/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.elasticsearch.{impl, _}
import akka.stream.scaladsl
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

import scala.collection.immutable

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
   * @param objectMapper Jackson object mapper converting type T` to JSON
   */
  def create[T, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper,
      paramsWriter: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed, P], NotUsed] =
    create(
      indexName,
      typeName,
      settings,
      elasticsearchClient,
      new JacksonMessageWriter[T](objectMapper),
      new JacksonParamsWriter[P](objectMapper)
    )

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   *
   * @param messageWriter converts type `T` to a `String` containing valid JSON
   */
  def create[T, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T],
      paramsWriter: MessageWriter[P]
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed,P], NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, NotUsed, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed, P](indexName, typeName, elasticsearchClient, settings, messageWriter, paramsWriter)
      )
      .mapConcat(identity)
      .asJava


  /**
    * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
    * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
    * successful execution.
    *
    * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
    *
    * @param messageWriter converts type `T` to a `String` containing valid JSON
    */
  def create[T, P](
                    indexName: String,
                    typeName: String,
                    settings: ElasticsearchWriteSettings,
                    elasticsearchClient: RestClient,
                    messageWriter: MessageWriter[T],
                    paramsWriter: MessageWriter[P],
                  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed,P], NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, NotUsed, NotUsed]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed, NotUsed](indexName, typeName, elasticsearchClient, settings, messageWriter, paramsWriter)
      )
      .mapConcat(identity)
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
  def createWithPassThrough[T, C, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper,
      paramsWriter: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, C, P], WriteResult[T, C, P], NotUsed] =
    createWithPassThrough(
      indexName,
      typeName,
      settings,
      elasticsearchClient,
      new JacksonMessageWriter[T](objectMapper),
      new JacksonMessageWriter[P](objectMapper)
    )

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
  def createWithPassThrough[T, C, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T],
      paramsWriter: MessageWriter[P]
  ): akka.stream.javadsl.Flow[WriteMessage[T, C, P], WriteResult[T, C, P], NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, C, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, C, P](indexName, typeName, elasticsearchClient, settings, messageWriter, paramsWriter)
      )
      .mapConcat(identity)
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
  def createWithContext[T, C, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper,
      paramsWriter: ObjectMapper
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed, P], C, WriteResult[T, C, P], C, NotUsed] =
    createWithContext(
      indexName,
      typeName,
      settings,
      elasticsearchClient,
      new JacksonMessageWriter[T](objectMapper),
      new JacksonParamsWriter[P](objectMapper)
    )

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
  def createWithContext[T, C, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      messageWriter: MessageWriter[T],
      paramsWriter: MessageWriter[P]
  ): akka.stream.javadsl.FlowWithContext[WriteMessage[T, NotUsed, P], C, WriteResult[T, C, P], C, NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, C, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, C, P](indexName, typeName, elasticsearchClient, settings, messageWriter, paramsWriter)
      )
      .mapConcat(identity)
      .asFlowWithContext[WriteMessage[T, NotUsed, P], C, C]((res, c) => res.withPassThrough(c))(
        p => p.message.passThrough
      )
      .asJava

  private final class JacksonMessageWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

  private final class JacksonParamsWriter[P](mapper: ObjectMapper) extends MessageWriter[P] {

    override def convert(message: P): String =
      mapper.writeValueAsString(message)
  }

}
