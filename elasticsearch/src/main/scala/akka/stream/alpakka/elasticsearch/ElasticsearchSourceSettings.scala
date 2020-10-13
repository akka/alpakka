/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import akka.util.JavaDurationConverters._

/**
 * Configure Elastiscsearch sources.
 */
final class ElasticsearchSourceSettings private (val bufferSize: Int,
                                                 val includeDocumentVersion: Boolean,
                                                 val scrollDuration: FiniteDuration
) {

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

  private def copy(bufferSize: Int = bufferSize,
                   includeDocumentVersion: Boolean = includeDocumentVersion,
                   scrollDuration: FiniteDuration = scrollDuration
  ): ElasticsearchSourceSettings =
    new ElasticsearchSourceSettings(bufferSize = bufferSize,
                                    includeDocumentVersion = includeDocumentVersion,
                                    scrollDuration = scrollDuration
    )

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
    s"""ElasticsearchSourceSettings(bufferSize=$bufferSize,includeDocumentVersion=$includeDocumentVersion,scrollDuration=$scrollDuration)"""

}

object ElasticsearchSourceSettings {

  val Default = new ElasticsearchSourceSettings(
    bufferSize = 10,
    includeDocumentVersion = false,
    scrollDuration = FiniteDuration(5, TimeUnit.MINUTES)
  )

  /** Scala API */
  def apply(): ElasticsearchSourceSettings = Default

  /** Java API */
  def create(): ElasticsearchSourceSettings = Default
}
