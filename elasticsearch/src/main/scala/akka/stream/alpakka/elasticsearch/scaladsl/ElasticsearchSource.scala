/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchSourceSettings, ElasticsearchSourceStage, OutgoingMessage}
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestClient

object ElasticsearchSource {

  /**
   * Scala API: creates a [[ElasticsearchSourceStage]] for Elasticsearch using an [[RestClient]]
   */
  def apply(indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient): Source[OutgoingMessage, NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, settings))

}
