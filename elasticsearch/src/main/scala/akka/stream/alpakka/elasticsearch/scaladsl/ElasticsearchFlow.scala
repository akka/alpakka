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
   * Scala API: creates a [[ElasticsearchFlowStage]] using IncomingMessage (without cargo)
   */
  def create[T](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[IncomingMessage[T], Seq[IncomingMessageResult[T]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, IncomingMessage[T]](
          indexName,
          typeName,
          client,
          settings,
          new SprayJsonWriter[T]()(writer)
        )
      )
      .mapAsync(1)(identity)
      .map { x =>
        x.map { e =>
          IncomingMessageResult(e.message.source, e.success)
        }
      }

  /**
   * Scala API: creates a [[ElasticsearchFlowStage]] using IncomingMessageWithCargo
   */
  def createWithCargo[T, C](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Flow[IncomingMessageWithCargo[T, C], Seq[IncomingMessageWithCargoResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchFlowStage[T, IncomingMessageWithCargo[T, C]](
          indexName,
          typeName,
          client,
          settings,
          new SprayJsonWriter[T]()(writer)
        )
      )
      .mapAsync(1)(identity)
      .map { x =>
        x.map { e =>
          IncomingMessageWithCargoResult(e.message.source, e.message.cargo, e.success)
        }
      }

  private class SprayJsonWriter[T](implicit writer: JsonWriter[T]) extends MessageWriter[T] {
    override def convert(message: T): String = message.toJson.toString()
  }

}
