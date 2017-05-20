/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.elasticsearch.{ElasticsearchFlowStage, ElasticsearchSinkSettings, IncomingMessage}
import akka.stream.javadsl.Sink
import org.elasticsearch.client.RestClient
import spray.json.{JsObject, JsonWriter}

import scala.compat.java8.FutureConverters.FutureOps

class ElasticsearchSink {

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSinkSettings,
             client: RestClient): Sink[IncomingMessage[JsObject], CompletionStage[Done]] =
    akka.stream.alpakka.elasticsearch.scaladsl
      .ElasticsearchSink(indexName, typeName, settings)(client)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSinkSettings,
               client: RestClient,
               writer: JsonWriter[T]): Sink[IncomingMessage[T], CompletionStage[Done]] =
    akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink
      .typed(indexName, typeName, settings)(client, writer)
      .mapMaterializedValue(_.toJava)
      .asJava

}
