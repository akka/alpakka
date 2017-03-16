/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchSourceSettings, ElasticsearchSourceStage, OutgoingMessage}
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestClient

class ElasticsearchSource {

  /**
   * Java API: creates a [[ElasticsearchSourceStage]] for Elasticsearch using an [[RestClient]]
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings,
             client: RestClient): Source[OutgoingMessage, NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, settings))

  /**
   * Java API: creates a [[ElasticsearchSourceStage]] for Elasticsearch using an [[RestClient]] with default settings.
   */
  def simple(indexName: String,
             typeName: String,
             query: String,
             client: RestClient): Source[OutgoingMessage, NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, ElasticsearchSourceSettings()))

}
