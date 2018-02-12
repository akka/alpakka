/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import scala.concurrent.duration.{FiniteDuration, _}

//#solr-update-settings
final case class SolrUpdateSettings(
    bufferSize: Int = 10,
    retryInterval: FiniteDuration = 5000.millis,
    maxRetry: Int = 100,
    commitWithin: Int = -1
) {
  def withBufferSize(bufferSize: Int): SolrUpdateSettings =
    copy(bufferSize = bufferSize)

  def withRetryInterval(retryInterval: FiniteDuration): SolrUpdateSettings =
    copy(retryInterval = retryInterval)

  def withMaxRetry(maxRetry: Int): SolrUpdateSettings =
    copy(maxRetry = maxRetry)

  def withCommitWithin(commitWithin: Int): SolrUpdateSettings =
    copy(commitWithin = commitWithin)
}
//#solr-update-settings

object SolrUpdateSettings {

  /**
   * Java API
   */
  def create(): SolrUpdateSettings = SolrUpdateSettings()
}
