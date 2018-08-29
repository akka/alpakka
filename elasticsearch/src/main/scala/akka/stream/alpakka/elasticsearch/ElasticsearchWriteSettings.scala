/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

/**
 * Configure Elasticsearch sinks and flows.
 */
final class ElasticsearchWriteSettings private (val bufferSize: Int,
                                                val retryInterval: scala.concurrent.duration.FiniteDuration,
                                                val maxRetry: Int,
                                                val retryPartialFailure: Boolean,
                                                val versionType: Option[String]) {

  def withBufferSize(value: Int): ElasticsearchWriteSettings = copy(bufferSize = value)

  /** Scala API */
  def withRetryInterval(value: scala.concurrent.duration.FiniteDuration): ElasticsearchWriteSettings =
    copy(retryInterval = value)

  /** Java API */
  def withRetryInterval(value: java.time.Duration): ElasticsearchWriteSettings =
    withRetryInterval(value.asScala)
  def withMaxRetry(value: Int): ElasticsearchWriteSettings = copy(maxRetry = value)

  /**
   * Note: If using retryPartialFailure == true, you will receive
   * messages out of order downstream in cases where
   * elastic returns error one some of the documents in a
   * bulk request.
   */
  def withRetryPartialFailure(value: Boolean): ElasticsearchWriteSettings =
    if (retryPartialFailure == value) this else copy(retryPartialFailure = value)

  def withVersionType(value: String): ElasticsearchWriteSettings = copy(versionType = Option(value))

  private def copy(bufferSize: Int = bufferSize,
                   retryInterval: scala.concurrent.duration.FiniteDuration = retryInterval,
                   maxRetry: Int = maxRetry,
                   retryPartialFailure: Boolean = retryPartialFailure,
                   versionType: Option[String] = versionType): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(bufferSize = bufferSize,
                                   retryInterval = retryInterval,
                                   maxRetry = maxRetry,
                                   retryPartialFailure = retryPartialFailure,
                                   versionType = versionType)

  override def toString =
    s"ElasticsearchUpdateSettings(bufferSize=$bufferSize,retryInterval=$retryInterval,maxRetry=$maxRetry,retryPartialFailure=$retryPartialFailure,versionType=$versionType)"

}

object ElasticsearchWriteSettings {
  val Default = new ElasticsearchWriteSettings(bufferSize = 10,
                                               retryInterval = 5.seconds,
                                               maxRetry = 100,
                                               retryPartialFailure = false,
                                               versionType = None)

  /** Scala API */
  def apply(): ElasticsearchWriteSettings = Default

  /** Java API */
  def create(): ElasticsearchWriteSettings = Default
}
