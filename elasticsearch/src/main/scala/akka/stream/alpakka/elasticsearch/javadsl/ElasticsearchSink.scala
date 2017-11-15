/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl._
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

object ElasticsearchSink {

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient
  ): akka.stream.javadsl.Sink[IncomingMessage[java.util.Map[String, Object]], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(indexName, typeName, settings, client)
      .toMat(Sink.ignore, Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as JsObject
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchSinkSettings,
      client: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Sink[IncomingMessage[java.util.Map[String, Object]], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(indexName, typeName, settings, client, objectMapper)
      .toMat(Sink.ignore, Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSinkSettings,
               client: RestClient): akka.stream.javadsl.Sink[IncomingMessage[T], CompletionStage[Done]] =
    ElasticsearchFlow
      .typed(indexName, typeName, settings, client)
      .toMat(Sink.ignore, Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a sink based on [[ElasticsearchFlowStage]] that accepts as specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSinkSettings,
               client: RestClient,
               objectMapper: ObjectMapper): akka.stream.javadsl.Sink[IncomingMessage[T], CompletionStage[Done]] =
    ElasticsearchFlow
      .typed(indexName, typeName, settings, client, objectMapper)
      .toMat(Sink.ignore, Keep.right[NotUsed, CompletionStage[Done]])

}
