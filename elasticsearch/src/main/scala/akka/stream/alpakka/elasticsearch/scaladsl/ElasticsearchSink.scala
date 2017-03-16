/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch.{ElasticsearchSinkSettings, ElasticsearchSinkStage, IncomingMessage}
import akka.stream.scaladsl.Sink
import org.elasticsearch.client.RestClient

object ElasticsearchSink {

  /**
   * Scala API: creates a [[ElasticsearchSinkStage]] for Elasticsearch using an [[RestClient]]
   */
  def apply(indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient): Sink[IncomingMessage, NotUsed] =
    Sink.fromGraph(new ElasticsearchSinkStage(indexName, typeName, client, settings))

}
