/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import scala.concurrent.duration._
import scala.collection.immutable
import akka.util.JavaDurationConverters._

trait RetryLogic {
  def shouldRetry(retries: Int, errors: immutable.Seq[String]): Boolean
  def nextRetry(retries: Int): FiniteDuration
}

object RetryNever extends RetryLogic {
  override def shouldRetry(retries: Int, errors: immutable.Seq[String]): Boolean = false
  override def nextRetry(retries: Int): FiniteDuration = Duration.Zero
}

/**
 * Note: If using retries, you will receive
 * messages out of order downstream in cases where
 * elastic returns error one some of the documents in a
 * bulk request.
 */
final class RetryAtFixedRate private (maxRetries: Int, retryInterval: scala.concurrent.duration.FiniteDuration)
    extends RetryLogic {
  override def shouldRetry(retries: Int, errors: immutable.Seq[String]): Boolean = retries < maxRetries
  override def nextRetry(retries: Int): FiniteDuration = retryInterval
}

object RetryAtFixedRate {

  def apply(maxRetries: Int, retryInterval: scala.concurrent.duration.FiniteDuration): RetryAtFixedRate =
    new RetryAtFixedRate(maxRetries, retryInterval)

  def create(maxRetries: Int, retryInterval: java.time.Duration): RetryAtFixedRate =
    new RetryAtFixedRate(maxRetries, retryInterval.asScala)
}

/**
 * Configure Elasticsearch sinks and flows.
 */
final class ElasticsearchWriteSettings private (val bufferSize: Int,
                                                val retryLogic: RetryLogic,
                                                val versionType: Option[String]) {

  def withBufferSize(value: Int): ElasticsearchWriteSettings = copy(bufferSize = value)

  def withRetryLogic(value: RetryLogic): ElasticsearchWriteSettings =
    copy(retryLogic = value)

  def withVersionType(value: String): ElasticsearchWriteSettings = copy(versionType = Option(value))

  private def copy(bufferSize: Int = bufferSize,
                   retryLogic: RetryLogic = retryLogic,
                   versionType: Option[String] = versionType): ElasticsearchWriteSettings =
    new ElasticsearchWriteSettings(bufferSize = bufferSize, retryLogic = retryLogic, versionType = versionType)

  override def toString =
    s"ElasticsearchUpdateSettings(bufferSize=$bufferSize,retryLogic=$retryLogic,versionType=$versionType)"

}

object ElasticsearchWriteSettings {
  val Default = new ElasticsearchWriteSettings(bufferSize = 10, retryLogic = RetryNever, versionType = None)

  /** Scala API */
  def apply(): ElasticsearchWriteSettings = Default

  /** Java API */
  def create(): ElasticsearchWriteSettings = Default
}
