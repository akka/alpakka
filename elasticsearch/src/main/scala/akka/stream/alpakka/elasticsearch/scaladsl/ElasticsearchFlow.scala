/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.annotation.{ApiMayChange, InternalApi}
import akka.stream.alpakka.elasticsearch.impl.backport.RetryFlow
import akka.stream.alpakka.elasticsearch.{impl, _}
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
   * Create a flow to update Elasticsearch with
   * [[immutable.Seq[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the immutable.Seq[[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]
   * and must be checked for successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   */
  def createBulk[T, C](indexName: String,
                       typeName: String,
                       settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit elasticsearchClient: RestClient,
      sprayJsonWriter: JsonWriter[T]
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
    createBulk[T, C](indexName, typeName, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with
   * [[immutable.Seq[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the immutable.Seq[[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]
   * and must be checked for successful execution.
   */
  def createBulk[T, C](indexName: String,
                       typeName: String,
                       settings: ElasticsearchWriteSettings,
                       writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
    Flow[WriteMessage[T, C]]
      .via(stageFlow(indexName, typeName, settings, elasticsearchClient, writer))
      .mapConcat(identity)

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
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
   */
  @ApiMayChange
  def createWithContext[T, C](indexName: String,
                              typeName: String,
                              settings: ElasticsearchWriteSettings,
                              writer: MessageWriter[T])(
      implicit elasticsearchClient: RestClient
  ): FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] = {
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(indexName, typeName, settings, elasticsearchClient, writer))
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
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] = {
    if (settings.retryLogic == RetryNever) {
      val basicFlow = basicStageFlow[T, C](indexName, typeName, settings, elasticsearchClient, writer)
      Flow[immutable.Seq[WriteMessage[T, C]]]
        .map(messages => messages -> immutable.Seq.empty[WriteResult[T, C]])
        .via(basicFlow)
    } else {
      def retryLogic(
          results: immutable.Seq[WriteResult[T, (Int, C)]]
      ): Option[(immutable.Seq[WriteMessage[T, (Int, C)]], immutable.Seq[WriteResult[T, (Int, C)]])] = {
        val (successful, failed) = results.partition(_.success)

        failed match {
          case Nil => None
          case failedResults => Some(failedResults.map(_.message) -> successful)
        }
      }

      val basicFlow = basicStageFlow[T, (Int, C)](indexName, typeName, settings, elasticsearchClient, writer)
      val retryFlow = RetryFlow.withBackoff(settings.retryLogic.minBackoff,
                                            settings.retryLogic.maxBackoff,
                                            0,
                                            settings.retryLogic.maxRetries,
                                            basicFlow) { (_, results) =>
        retryLogic(results)
      }

      amendWithIndexFlow[T, C]
        .via(retryFlow)
        .via(applyOrderingFlow[T, C])
    }
  }

  @InternalApi
  private def amendWithIndexFlow[T, C]
      : Flow[immutable.Seq[WriteMessage[T, C]],
             (immutable.Seq[WriteMessage[T, (Int, C)]], immutable.Seq[WriteResult[T, (Int, C)]]),
             NotUsed] = {
    Flow[immutable.Seq[WriteMessage[T, C]]].map { messages =>
      val indexedMessages = messages.zipWithIndex.map {
        case (m, idx) =>
          m.withPassThrough(idx -> m.passThrough)
      }
      indexedMessages -> Nil
    }
  }

  @InternalApi
  private def applyOrderingFlow[T, C]
      : Flow[immutable.Seq[WriteResult[T, (Int, C)]], immutable.Seq[WriteResult[T, C]], NotUsed] = {
    Flow[immutable.Seq[WriteResult[T, (Int, C)]]].map { results =>
      val orderedResults = results.sortBy(_.message.passThrough._1)
      val finalResults = orderedResults.map { r =>
        new WriteResult(r.message.withPassThrough(r.message.passThrough._2), r.error)
      }
      finalResults
    }
  }

  @InternalApi
  private def basicStageFlow[T, C](indexName: String,
                                   typeName: String,
                                   settings: ElasticsearchWriteSettings,
                                   elasticsearchClient: RestClient,
                                   writer: MessageWriter[T]) = {
    Flow.fromGraph {
      new impl.ElasticsearchSimpleFlowStage[T, C](indexName, typeName, elasticsearchClient, settings, writer)
    }
  }

  private final class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
