/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestClient

object ElasticsearchSource {

  def apply(indexName: String, typeName: String, query: String, settings: ElasticsearchSourceSettings)(
      implicit client: RestClient): Source[Map[String, Any], NotUsed] =
    Source.fromGraph(new ElasticsearchSourceStage(indexName, typeName, query, client, settings))

}
