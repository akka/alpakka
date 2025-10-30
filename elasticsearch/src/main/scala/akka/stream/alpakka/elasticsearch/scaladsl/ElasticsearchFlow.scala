/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.annotation.{ApiMayChange, InternalApi}
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.{Flow, FlowWithContext, RetryFlow}
import spray.json._

import scala.collection.immutable
import scala.concurrent.ExecutionContext

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
  def create[T](elasticsearchParams: ElasticsearchParams, settings: WriteSettingsBase[_, _])(
      implicit sprayJsonWriter: JsonWriter[T]
  ): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    create[T](elasticsearchParams, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   */
  def create[T](elasticsearchParams: ElasticsearchParams,
                settings: WriteSettingsBase[_, _],
                writer: MessageWriter[T]): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] = {
    Flow[WriteMessage[T, NotUsed]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(elasticsearchParams, settings, writer))
      .mapConcat(identity)
  }

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   */
  def createWithPassThrough[T, C](elasticsearchParams: ElasticsearchParams, settings: WriteSettingsBase[_, _])(
      implicit sprayJsonWriter: JsonWriter[T]
  ): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    createWithPassThrough[T, C](elasticsearchParams, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   */
  def createWithPassThrough[T, C](elasticsearchParams: ElasticsearchParams,
                                  settings: WriteSettingsBase[_, _],
                                  writer: MessageWriter[T]): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] = {
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(elasticsearchParams, settings, writer))
      .mapConcat(identity)
  }

  /**
   * Create a flow to update Elasticsearch with
   * [[immutable.Seq[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the immutable.Seq[[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]
   * and must be checked for successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   */
  def createBulk[T, C](elasticsearchParams: ElasticsearchParams, settings: WriteSettingsBase[_, _])(
      implicit sprayJsonWriter: JsonWriter[T]
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] =
    createBulk[T, C](elasticsearchParams, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with
   * [[immutable.Seq[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is part of the immutable.Seq[[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]]
   * and must be checked for successful execution.
   */
  def createBulk[T, C](
      elasticsearchParams: ElasticsearchParams,
      settings: WriteSettingsBase[_, _],
      writer: MessageWriter[T]
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] = {
    stageFlow(elasticsearchParams, settings, writer)
  }

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * This factory method requires an implicit Spray JSON writer for `T`.
   */
  @ApiMayChange
  def createWithContext[T, C](elasticsearchParams: ElasticsearchParams, settings: WriteSettingsBase[_, _])(
      implicit sprayJsonWriter: JsonWriter[T]
  ): FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] =
    createWithContext[T, C](elasticsearchParams, settings, new SprayJsonWriter[T]()(sprayJsonWriter))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `context` of type `C`.
   * The result status is part of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   */
  @ApiMayChange
  def createWithContext[T, C](
      elasticsearchParams: ElasticsearchParams,
      settings: WriteSettingsBase[_, _],
      writer: MessageWriter[T]
  ): FlowWithContext[WriteMessage[T, NotUsed], C, WriteResult[T, C], C, NotUsed] = {
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(stageFlow(elasticsearchParams, settings, writer))
      .mapConcat(identity)
      .asFlowWithContext[WriteMessage[T, NotUsed], C, C]((res, c) => res.withPassThrough(c))(
        p => p.message.passThrough
      )
  }

  @InternalApi
  private def stageFlow[T, C](
      elasticsearchParams: ElasticsearchParams,
      settings: WriteSettingsBase[_, _],
      writer: MessageWriter[T]
  ): Flow[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], NotUsed] = {
    if (settings.retryLogic == RetryNever) {
      val basicFlow = basicStageFlow[T, C](elasticsearchParams, settings, writer)
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

      val basicFlow = basicStageFlow[T, (Int, C)](elasticsearchParams, settings, writer)
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
  private def basicStageFlow[T, C](elasticsearchParams: ElasticsearchParams,
                                   settings: WriteSettingsBase[_, _],
                                   writer: MessageWriter[T]) = {
    Flow
      .fromMaterializer { (mat, _) =>
        implicit val system: ActorSystem = mat.system
        implicit val http: HttpExt = Http()
        implicit val ec: ExecutionContext = mat.executionContext

        Flow.fromGraph {
          new impl.ElasticsearchSimpleFlowStage[T, C](elasticsearchParams, settings, writer)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private final class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
