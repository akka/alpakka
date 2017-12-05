/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.elasticsearch.IncomingMessage
import akka.stream.scaladsl.{Keep, Sink}
import org.elasticsearch.client.RestClient
import spray.json.JsonWriter

import scala.concurrent.Future

object ElasticsearchSink {

  /**
   * Scala API: creates a sink based on [[ElasticsearchFlowStage]]
   */
  def create[T](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient,
      writer: JsonWriter[T]
  ): Sink[IncomingMessage[T, NotUsed], Future[Done]] =
    ElasticsearchFlow.create[T](indexName, typeName, settings).toMat(Sink.ignore)(Keep.right)

}
