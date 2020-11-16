/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import java.util.concurrent.TimeUnit

import akka.util.JavaDurationConverters._

import scala.concurrent.duration.FiniteDuration

/**
 * Configure Elastiscsearch sources.
 *
 */
final class ElasticsearchSourceSettings private (val connection: ElasticsearchConnectionSettings,
                                                 val bufferSize: Int,
                                                 val includeDocumentVersion: Boolean,
                                                 val scrollDuration: FiniteDuration,
                                                 val apiVersion: ApiVersion) {

  def withConnection(value: ElasticsearchConnectionSettings): ElasticsearchSourceSettings = copy(connection = value)

  def withBufferSize(value: Int): ElasticsearchSourceSettings = copy(bufferSize = value)

  def withScrollDuration(value: FiniteDuration): ElasticsearchSourceSettings = copy(scrollDuration = value)

  def withScrollDuration(value: java.time.Duration): ElasticsearchSourceSettings = copy(scrollDuration = value.asScala)

  /**
   * If includeDocumentVersion is true, '_version' is returned with the search-results
   *  * http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html
   *  * https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
   */
  def withIncludeDocumentVersion(value: Boolean): ElasticsearchSourceSettings =
    if (includeDocumentVersion == value) this else copy(includeDocumentVersion = value)

  def withApiVersion(value: ApiVersion): ElasticsearchSourceSettings =
    if (apiVersion == value) this else copy(apiVersion = value)

  private def copy(connection: ElasticsearchConnectionSettings = connection,
                   bufferSize: Int = bufferSize,
                   includeDocumentVersion: Boolean = includeDocumentVersion,
                   scrollDuration: FiniteDuration = scrollDuration,
                   apiVersion: ApiVersion = apiVersion): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(connection = connection,
                                    bufferSize = bufferSize,
                                    includeDocumentVersion = includeDocumentVersion,
                                    scrollDuration = scrollDuration,
                                    apiVersion = apiVersion)

  def scroll: String = {
    val scrollString = scrollDuration.unit match {
      case TimeUnit.DAYS => "d"
      case TimeUnit.HOURS => "h"
      case TimeUnit.MINUTES => "m"
      case TimeUnit.SECONDS => "s"
      case TimeUnit.MILLISECONDS => "ms"
      case TimeUnit.MICROSECONDS => "micros"
      case TimeUnit.NANOSECONDS => "nanos"
    }

    s"${scrollDuration.length}$scrollString"
  }

  override def toString =
    s"""ElasticsearchSourceSettings(connection=$connection,bufferSize=$bufferSize,includeDocumentVersion=$includeDocumentVersion,scrollDuration=$scrollDuration,apiVersion=$apiVersion)"""

}

object ElasticsearchSourceSettings {

  /** Scala API */
  def apply(connection: ElasticsearchConnectionSettings): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(connection,
                                    10,
                                    includeDocumentVersion = false,
                                    FiniteDuration(5, TimeUnit.MINUTES),
                                    ApiVersion.V7)

  /** Java API */
  def create(connection: ElasticsearchConnectionSettings): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(connection,
                                    10,
                                    includeDocumentVersion = false,
                                    FiniteDuration(5, TimeUnit.MINUTES),
                                    ApiVersion.V7)
}
