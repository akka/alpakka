/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl
import akka.stream.scaladsl.Flow
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
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T](indexName: String,
                typeName: String,
                settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    create[T](indexName, typeName, settings, new SprayJsonWriter[T]()(writer))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T](indexName: String, typeName: String, settings: ElasticsearchWriteSettings, writer: MessageWriter[T])(
      implicit client: RestClient
  ): Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    Flow[WriteMessage[T, NotUsed]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed](
          indexName,
          typeName,
          client,
          settings,
          writer
        )
      )
      .mapConcat(identity)

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings = ElasticsearchWriteSettings.Default)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    createWithPassThrough[T, C](indexName, typeName, settings, new SprayJsonWriter[T]()(writer))

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C](indexName: String,
                                  typeName: String,
                                  settings: ElasticsearchWriteSettings,
                                  writer: MessageWriter[T])(
      implicit client: RestClient
  ): Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, C](
          indexName,
          typeName,
          client,
          settings,
          writer
        )
      )
      .mapConcat(identity)

  private final class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
