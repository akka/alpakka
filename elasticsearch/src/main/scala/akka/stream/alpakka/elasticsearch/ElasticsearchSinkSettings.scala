/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

object ElasticsearchSinkSettings {
  val Default = ElasticsearchSinkSettings()
}

/**
 * Configure Elasticsearch sinks.
 *
 * Note: If using retryPartialFailure == true, you will receive
 * messages out of order downstream in cases where
 * elastic returns error one some of the documents in a
 * bulk request.
 */
final case class ElasticsearchSinkSettings(bufferSize: Int = 10,
                                           retryInterval: Int = 5000,
                                           maxRetry: Int = 100,
                                           retryPartialFailure: Boolean = false,
                                           versionType: Option[String] = None) {
  def withBufferSize(bufferSize: Int): ElasticsearchSinkSettings =
    copy(bufferSize = bufferSize)

  def withRetryInterval(retryInterval: Int): ElasticsearchSinkSettings =
    copy(retryInterval = retryInterval)

  def withMaxRetry(maxRetry: Int): ElasticsearchSinkSettings =
    copy(maxRetry = maxRetry)

  def withRetryPartialFailure(retryPartialFailure: Boolean): ElasticsearchSinkSettings =
    copy(retryPartialFailure = retryPartialFailure)

  def withVersionType(versionType: String): ElasticsearchSinkSettings =
    copy(versionType = Some(versionType))
}
