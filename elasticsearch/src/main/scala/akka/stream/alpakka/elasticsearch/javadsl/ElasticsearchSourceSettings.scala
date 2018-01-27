/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.stream.alpakka.elasticsearch._
import scaladsl.{ElasticsearchSourceSettings => ScalaElasticsearchSourceSettings}

/**
 * Java API to configure Elastiscsearch sources.
 *
 * If includeDocumentVersion is true, '_version' is returned with the search-results
 * http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html
 * https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
 */
final class ElasticsearchSourceSettings(
    val bufferSize: Int,
    val includeDocumentVersion: Boolean
) {

  def this() = this(10, false)

  def withBufferSize(bufferSize: Int): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(bufferSize, includeDocumentVersion)

  def withIncludeDocumentVersion(includeDocumentVersion: Boolean): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(bufferSize, includeDocumentVersion)

  private[javadsl] def asScala: ScalaElasticsearchSourceSettings =
    ScalaElasticsearchSourceSettings(
      this.bufferSize,
      this.includeDocumentVersion
    )
}
