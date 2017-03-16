/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchSinkSettings, ElasticsearchSinkStage, IncomingMessage}
import akka.stream.scaladsl.Sink
import org.elasticsearch.client.RestClient

object ElasticsearchSink {

  /**
   * Java API: creates a [[ElasticsearchSinkStage]] for Elasticsearch using an [[RestClient]]
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSinkSettings,
             client: RestClient): Sink[IncomingMessage, NotUsed] =
    Sink.fromGraph(new ElasticsearchSinkStage(indexName, typeName, client, settings))

  /**
   * Java API: creates a [[ElasticsearchSinkStage]] for Elasticsearch using an [[RestClient]] with default settings
   */
  def simple(indexName: String,
             typeName: String,
             client: RestClient): Sink[IncomingMessage, NotUsed] =
    Sink.fromGraph(new ElasticsearchSinkStage(indexName, typeName, client, ElasticsearchSinkSettings()))

}
