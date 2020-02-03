/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl._
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestClient

/**
 * Java API to create Elasticsearch sinks.
 */
object ElasticsearchSink {

  /**
   * Create a sink to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Sink[WriteMessage[T, NotUsed], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(indexName, typeName, settings, elasticsearchClient, objectMapper)
      .toMat(Sink.ignore[WriteResult[T, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

}
