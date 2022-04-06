/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.opensearch

import akka.stream.alpakka.elasticsearch.ElasticsearchParams

/**
 * Opensearch 1.x is fully compatible with Elasticsearch 7.x release line, so we could
 * reuse the Elasticsearch V7 compatibile implementation.
 */
object OpensearchParams {
  def V1(indexName: String): ElasticsearchParams = ElasticsearchParams.V7(indexName)
}
