/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
  def create[T, P](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      elasticsearchClient: RestClient,
      objectMapper: ObjectMapper,
      paramMapper: ObjectMapper
  ): akka.stream.javadsl.Sink[WriteMessage[T, NotUsed, P], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(indexName, typeName, settings, elasticsearchClient, objectMapper, paramMapper)
      .toMat(Sink.ignore[WriteResult[T, NotUsed, P]], Keep.right[NotUsed, CompletionStage[Done]])

}
