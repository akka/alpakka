/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

/**
 * Opensearch 1.x is fully compatible with Elasticsearch 7.x with respect to
 * connection parameters.
 */
object OpensearchConnectionSettings {

  /** Scala API */
  def apply(baseUrl: String): ElasticsearchConnectionSettings =
    ElasticsearchConnectionSettings.apply(baseUrl)

  /** Java API */
  def create(baseUrl: String): ElasticsearchConnectionSettings =
    ElasticsearchConnectionSettings.create(baseUrl)
}
