/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl._
import akka.{Done, NotUsed}
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Java API to create Elasticsearch sinks.
 */
object ElasticsearchSink {

  /**
   * Create a sink to update Elasticsearch with [[akka.stream.alpakka.elasticsearch.WriteMessage WriteMessage]]s containing type `T`.
   */
  def create[T](
      elasticsearchParams: ElasticsearchParams,
      settings: ElasticsearchWriteSettings,
      objectMapper: ObjectMapper
  ): akka.stream.javadsl.Sink[WriteMessage[T, NotUsed], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(elasticsearchParams, settings, objectMapper)
      .toMat(Sink.ignore[WriteResult[T, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

}
