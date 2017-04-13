/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.Done
import akka.stream.alpakka.elasticsearch.{ElasticsearchSinkSettings, ElasticsearchSinkStage, IncomingMessage}
import akka.stream.scaladsl.Sink
import org.elasticsearch.client.RestClient
import spray.json.{DefaultJsonProtocol, JsObject, JsonWriter}

import scala.concurrent.Future

object ElasticsearchSink {

  /**
   * Java API: creates a [[ElasticsearchSinkStage]] that consumes as JsObject
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSinkSettings,
             client: RestClient): Sink[IncomingMessage[JsObject], Future[Done]] =
    Sink.fromGraph(
      new ElasticsearchSinkStage(indexName, typeName, client, settings)(DefaultJsonProtocol.RootJsObjectFormat)
    )

  /**
   * Java API: creates a [[ElasticsearchSinkStage]] that consumes as specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               client: RestClient,
               settings: ElasticsearchSinkSettings,
               writer: JsonWriter[T]): Sink[IncomingMessage[T], Future[Done]] =
    Sink.fromGraph(new ElasticsearchSinkStage(indexName, typeName, client, settings)(writer))

}
