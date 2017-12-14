/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Flow
import org.elasticsearch.client.RestClient
import spray.json._

object ElasticsearchFlow {

  /**
   * Scala API: creates a [[ElasticsearchFlowStage]] without passThrough
   */
  def create[T](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, NotUsed](
          indexName,
          typeName,
          client,
          settings,
          new SprayJsonWriter[T]()(writer)
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[ElasticsearchFlowStage]] with passThrough
   */
  def createWithPassThrough[T, C](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, C](
          indexName,
          typeName,
          client,
          settings,
          new SprayJsonWriter[T]()(writer)
        )
      )
      .mapAsync(1)(identity)

  private class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
