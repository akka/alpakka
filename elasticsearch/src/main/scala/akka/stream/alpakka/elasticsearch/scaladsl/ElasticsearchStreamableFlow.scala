/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Flow
import org.elasticsearch.client.RestClient
import spray.json._

object ElasticsearchStreamableFlow {

  /**
   * Scala API: creates a [[ElasticsearchStreamableFlow]]
   */
  def apply[T <: StreamableIncomingMessage](indexName: String, typeName: String, settings: ElasticsearchSinkSettings)(
      implicit client: RestClient
  ): Flow[T, StreamableIncomingMessagesResult[T], NotUsed] =
    Flow
      .fromGraph(
        new ElasticsearchStreamableFlowStage[T](
          indexName,
          typeName,
          client,
          settings
        )
      )
      .mapAsync(1) { x =>
        x
      }
}
