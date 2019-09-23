/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl
import akka.stream.scaladsl.{Flow, FlowWithContext}
import org.elasticsearch.client.RestClient
import spray.json._

import scala.collection.immutable

/**
 * Scala API to create Elasticsearch flows.
 */
object ElasticsearchFlow {

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T, P](indexName: String,
                typeName: String,
                settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit elasticsearchClient: RestClient,
      sprayJsonDocWriter: JsonWriter[T],
      sprayJsonParamsWriter: JsonWriter[P]
  ): Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed, P], NotUsed] =
    create[T, P](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonDocWriter), new SprayJsonWriter[P]()(sprayJsonParamsWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T, P](indexName: String, typeName: String, settings: ElasticsearchWriteSettings, writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed, P], NotUsed] =
    Flow[WriteMessage[T, NotUsed, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed, P](
          indexName,
          typeName,
          elasticsearchClient,
          settings,
          writer,
          StringParamsWriter.getInstance()
        )
      )
      .mapConcat(identity)


  def create[T, P](indexName: String, typeName: String, settings: ElasticsearchWriteSettings, writer: MessageWriter[T], paramsWriter: MessageWriter[P])(
    implicit elasticsearchClient: RestClient
  ): Flow[WriteMessage[T, NotUsed, P], WriteResult[T, NotUsed, P], NotUsed] =
    Flow[WriteMessage[T, NotUsed, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed, P](
          indexName,
          typeName,
          elasticsearchClient,
          settings,
          writer,
          paramsWriter
        )
      )
      .mapConcat(identity)

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C, P](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T],
      paramsJsonWriter: JsonWriter[P],
  ): Flow[WriteMessage[T, C, P], WriteResult[T, C, P], NotUsed] =
    createWithPassThrough[T, C, P](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter), new SprayJsonWriter[P]()(paramsJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C, P](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings,
                                  writer: MessageWriter[T],
                                  paramWriter: MessageWriter[P])(
      implicit elasticsearchClient: RestClient
  ): Flow[WriteMessage[T, C, P], WriteResult[T, C, P], NotUsed] =
    Flow[WriteMessage[T, C, P]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, C, P](
          indexName,
          typeName,
          elasticsearchClient,
          settings,
          writer,
          paramWriter
        )
      )
      .mapConcat(identity)

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   *
   * @throws IllegalArgumentException When settings configure retrying.
   */
  @ApiMayChange
  def createWithContext[T, C, P](indexName: String,
                              typeName: String,
                              settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings())(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T],
      paramsJsonWriter: JsonWriter[P],
  ): FlowWithContext[WriteMessage[T, NotUsed, P], C, WriteResult[T, C, P], C, NotUsed] =
    createWithContext[T, C, P](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter), new SprayJsonWriter[P]()(paramsJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * @throws IllegalArgumentException When settings configure retrying.
   */
  @ApiMayChange
  def createWithContext[T, C, P](indexName: String,
                              typeName: String,
                              settings: ElasticsearchWriteSettings,
                              writer: MessageWriter[T],
                                paramsWriter: MessageWriter[P])(
      implicit elasticsearchClient: RestClient
  ): FlowWithContext[WriteMessage[T, NotUsed, P], C, WriteResult[T, C, P], C, NotUsed] = {
    require(settings.retryLogic == RetryNever,
            "`withContext` may not be used with retrying enabled, as it disturbs element order")
    Flow[WriteMessage[T, C, P]]
      .via(createWithPassThrough(indexName, typeName, settings, writer, paramsWriter))
      .asFlowWithContext[WriteMessage[T, NotUsed, P], C, C]((res, c) => res.withPassThrough(c))(
        p => p.message.passThrough
      )
  }

  private final class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

  private final class ParamsJsonWriter[P](implicit writer: JsonWriter[P]) extends MessageWriter[P] {
    override def convert(message: P): String = message.toJson.toString()
  }

}
