/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

trait RetryLogic {
  def maxRetries: Int
  def minBackoff: scala.concurrent.duration.FiniteDuration
  def maxBackoff: scala.concurrent.duration.FiniteDuration
}

object RetryNever extends RetryLogic {
  override def maxRetries: Int = 0
  override def minBackoff: scala.concurrent.duration.FiniteDuration = Duration.Zero
  override def maxBackoff: scala.concurrent.duration.FiniteDuration = Duration.Zero
}

final class RetryAtFixedRate(_maxRetries: Int, retryInterval: scala.concurrent.duration.FiniteDuration)
    extends RetryLogic {
  override val maxRetries: Int = _maxRetries
  override val minBackoff: scala.concurrent.duration.FiniteDuration = retryInterval
  override def maxBackoff: scala.concurrent.duration.FiniteDuration = retryInterval
}

object RetryAtFixedRate {

  def apply(maxRetries: Int, retryInterval: scala.concurrent.duration.FiniteDuration): RetryAtFixedRate =
    new RetryAtFixedRate(maxRetries, retryInterval)

  def create(maxRetries: Int, retryInterval: java.time.Duration): RetryAtFixedRate =
    new RetryAtFixedRate(maxRetries, retryInterval.asScala)
}

final class RetryWithBackoff(_maxRetries: Int,
                             _minBackoff: scala.concurrent.duration.FiniteDuration,
                             _maxBackoff: scala.concurrent.duration.FiniteDuration)
    extends RetryLogic {
  override val maxRetries: Int = _maxRetries
  override val minBackoff: scala.concurrent.duration.FiniteDuration = _minBackoff
  override def maxBackoff: scala.concurrent.duration.FiniteDuration = _maxBackoff
}

object RetryWithBackoff {

  def apply(maxRetries: Int,
            minBackoff: scala.concurrent.duration.FiniteDuration,
            maxBackoff: scala.concurrent.duration.FiniteDuration): RetryWithBackoff =
    new RetryWithBackoff(maxRetries, minBackoff, maxBackoff)

  def create(maxRetries: Int, minBackoff: java.time.Duration, maxBackoff: java.time.Duration): RetryWithBackoff =
    new RetryWithBackoff(maxRetries, minBackoff.asScala, maxBackoff.asScala)
}

/**
 * Configure Elasticsearch sinks and flows.
 */
final class ElasticsearchWriteSettings private (val bufferSize: Int,
                                                val retryLogic: RetryLogic,
                                                val versionType: Option[String],
                                                val apiVersion: ApiVersion) {

  def withBufferSize(value: Int): ElasticsearchWriteSettings = copy(bufferSize = value)

  def withRetryLogic(value: RetryLogic): ElasticsearchWriteSettings =
    copy(retryLogic = value)

  def withVersionType(value: String): ElasticsearchWriteSettings = copy(versionType = Option(value))

  def withApiVersion(value: ApiVersion): ElasticsearchWriteSettings =
    if (apiVersion == value) this else copy(apiVersion = value)

  private def copy(bufferSize: Int = bufferSize,
                   retryLogic: RetryLogic = retryLogic,
                   versionType: Option[String] = versionType,
                   apiVersion: ApiVersion = apiVersion): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(bufferSize, retryLogic, versionType, apiVersion)

  override def toString =
    "ElasticsearchUpdateSettings(" +
    s"bufferSize=$bufferSize," +
    s"retryLogic=$retryLogic," +
    s"versionType=$versionType," +
    s"apiVersion=$apiVersion)"

}

object ElasticsearchWriteSettings {
  val Default = new ElasticsearchWriteSettings(bufferSize = 10,
                                               retryLogic = RetryNever,
                                               versionType = None,
                                               apiVersion = ApiVersion.V5)

  /** Scala API */
  def apply(): ElasticsearchWriteSettings = Default

  /** Java API */
  def create(): ElasticsearchWriteSettings = Default
}
