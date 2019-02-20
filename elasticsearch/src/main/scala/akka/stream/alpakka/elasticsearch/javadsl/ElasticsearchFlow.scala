/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

import akka.stream.alpakka.elasticsearch.impl

import scala.collection.immutable

/**
 * Java API to create Elasticsearch flows.
 */
object ElasticsearchFlow {

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], WriteResult[T, NotUsed], NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, NotUsed]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, NotUsed](indexName,
                                                    typeName,
                                                    client,
                                                    settings,
                                                    new JacksonWriter[T](objectMapper))
      )
      .mapConcat(identity)
      .asJava

  /**
   * Create a flow to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`
   * with `passThrough` of type `C`.
   * The result status is port of the [[akka.stream.alpakka.elasticsearch.WriteResult WriteResult]] and must be checked for
   * successful execution.
   *
   * Warning: When settings configure retrying, messages are emitted out-of-order when errors are detected.
   */
  def createWithPassThrough[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], WriteResult[T, C], NotUsed] =
    scaladsl
      .Flow[WriteMessage[T, C]]
      .batch(settings.bufferSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.ElasticsearchFlowStage[T, C](indexName, typeName, client, settings, new JacksonWriter[T](objectMapper))
      )
      .mapConcat(identity)
      .asJava

  private final class JacksonWriter[T](mapper: ObjectMapper) extends MessageWriter[T] {

    override def convert(message: T): String =
      mapper.writeValueAsString(message)
  }

}
