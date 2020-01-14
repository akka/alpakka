/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.annotation.{ApiMayChange, InternalApi}
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl
import akka.stream.scaladsl.{Flow, FlowWithContext}
import org.elasticsearch.client.RestClient
import spray.json._

import scala.collection.immutable
import scala.util.{Failure, Success}

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
  def create[T](indexName: String,
                typeName: String,
                settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T]
  ): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    create[T](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T](indexName: String, typeName: String, settings: ElasticsearchWriteSettings, writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    Flow[WriteMessage[T, NotUsed]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(indexName, typeName, settings, elasticsearchClient, writer))
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
  def createWithPassThrough[T, C](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T]
  ): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    createWithPassThrough[T, C](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings,
                                  writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(indexName, typeName, settings, elasticsearchClient, writer))
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
  def createWithContext[T, C](indexName: String,
                              typeName: String,
                              settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings())(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T]
  ): FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    createWithContext[T, C](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * @throws IllegalArgumentException When settings configure retrying.
   */
  @ApiMayChange
  def createWithContext[T, C](indexName: String,
                              typeName: String,
                              settings: ElasticsearchWriteSettings,
                              writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] = {
    require(settings.retryLogic == RetryNever,
            "`withContext` may not be used with retrying enabled, as it disturbs element order")
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(simpleStageFlow(indexName, typeName, settings, elasticsearchClient, writer))
      .mapConcat(identity)
      .asFlowWithContext[WriteMessage[T, NotUsed], C, C]((res, c) => res.withPassThrough(c))(
        p => p.message.passThrough
      )
  }

  @InternalApi
  private def stageFlow[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      writer: MessageWriter[T]
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
    if (settings.retryLogic == RetryNever) simpleStageFlow(indexName, typeName, settings, elasticsearchClient, writer)
    else {
      Flow.fromGraph(
        new impl.ElasticsearchFlowStage[T, C](
          indexName,
          typeName,
          elasticsearchClient,
          settings,
          writer
        )
      )
    }

  @InternalApi
  private def simpleStageFlow[C, T](indexName: String,
                                    typeName: String,
                                    settings: ElasticsearchWriteSettings,
                                    elasticsearchClient: RestClient,
                                    writer: MessageWriter[T]) =
    Flow
      .fromGraph(
        new impl.ElasticsearchSimpleFlowStage[T, C](indexName, typeName, elasticsearchClient, settings, writer)
      )
      .map {
        case Success(value) => value
        case Failure(e) => throw e
      }

  private final class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
