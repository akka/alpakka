/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

/**
 * Configure Elastiscsearch sources.
 *
 */
final class ElasticsearchSourceSettings private (val bufferSize: Int,
                                                 val includeDocumentVersion: Boolean,
                                                 val scroll: String) {

  def withBufferSize(value: Int): ElasticsearchSourceSettings = copy(bufferSize = value)

  def withScroll(value: String): ElasticsearchSourceSettings = copy(scroll = value)

  /**
   * If includeDocumentVersion is true, '_version' is returned with the search-results
   *  * http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html
   *  * https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
   */
  def withIncludeDocumentVersion(value: Boolean): ElasticsearchSourceSettings =
    if (includeDocumentVersion == value) this else copy(includeDocumentVersion = value)

  private def copy(bufferSize: Int = bufferSize,
                   includeDocumentVersion: Boolean = includeDocumentVersion,
                   scroll: String = scroll): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(bufferSize = bufferSize,
                                    includeDocumentVersion = includeDocumentVersion,
                                    scroll = scroll)

  override def toString =
    s"""ElasticsearchSourceSettings(bufferSize=$bufferSize,includeDocumentVersion=$includeDocumentVersion)"""

}

object ElasticsearchSourceSettings {

  val Default = new ElasticsearchSourceSettings(bufferSize = 10, includeDocumentVersion = false, scroll = "5m")

  /** Scala API */
  def apply(): ElasticsearchSourceSettings = Default

  /** Java API */
  def create(): ElasticsearchSourceSettings = Default
}
