/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

/**
 * Scala API to configure Elastiscsearch sources.
  *
  * If includeDocumentVersion is true, '_version' is returned with the search-results
  * http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html
  * https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
 */
//#source-settings
final case class ElasticsearchSourceSettings
(
  bufferSize: Int = 10,
  includeDocumentVersion:Boolean = false
)
//#source-settings
