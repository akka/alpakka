/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.util.JavaDurationConverters._

import scala.concurrent.duration._

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
final class ElasticsearchWriteSettings private (connection: ElasticsearchConnectionSettings,
                                                bufferSize: Int,
                                                retryLogic: RetryLogic,
                                                versionType: Option[String],
                                                apiVersion: ApiVersion,
                                                allowExplicitIndex: Boolean)
    extends WriteSettingsBase[ApiVersion, ElasticsearchWriteSettings](connection,
                                                                      bufferSize,
                                                                      retryLogic,
                                                                      versionType,
                                                                      apiVersion,
                                                                      allowExplicitIndex) {

  protected override def copy(connection: ElasticsearchConnectionSettings,
                              bufferSize: Int,
                              retryLogic: RetryLogic,
                              versionType: Option[String],
                              apiVersion: ApiVersion,
                              allowExplicitIndex: Boolean): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(connection, bufferSize, retryLogic, versionType, apiVersion, allowExplicitIndex)

  override def toString: String =
    "ElasticsearchWriteSettings(" +
    s"connection=$connection," +
    s"bufferSize=$bufferSize," +
    s"retryLogic=$retryLogic," +
    s"versionType=$versionType," +
    s"apiVersion=$apiVersion," +
    s"allowExplicitIndex=$allowExplicitIndex)"

}

object ElasticsearchWriteSettings {

  /** Scala API */
  def apply(connection: ElasticsearchConnectionSettings): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(connection, 10, RetryNever, None, ApiVersion.V7, allowExplicitIndex = true)

  /** Java API */
  def create(connection: ElasticsearchConnectionSettings): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(connection, 10, RetryNever, None, ApiVersion.V7, allowExplicitIndex = true)
}
