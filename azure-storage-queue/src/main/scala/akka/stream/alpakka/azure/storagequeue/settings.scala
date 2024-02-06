/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.azure.storagequeue

import java.time.{Duration => JavaDuration}
import java.util.Optional

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Settings for AzureQueueSource
 *
 * @param initalVisibilityTimeout Specifies how many seconds a message becomes invisible after it has been dequeued.
 *        See parameter of the same name in [[com.microsoft.azure.storage.queue.CloudQueue$.retrieveMessages]].
 * @param batchSize Specifies how many message are fetched in one batch.
 *        (This is the numberOfMessages parameter in [[com.microsoft.azure.storage.queue.CloudQueue$.retrieveMessages]].)
 * @param retrieveRetryTimeout If None the [[akka.stream.alpakka.azure.storagequeue.scaladsl.AzureQueueSource]] will be completed if the queue is empty.
 *        If Some(timeout) [[akka.stream.alpakka.azure.storagequeue.scaladsl.AzureQueueSource]] will retry after timeout to get new messages. Do not set timeout to low.
 */
final class AzureQueueSourceSettings private (
    val initialVisibilityTimeout: Int,
    val batchSize: Int,
    val retrieveRetryTimeout: Option[FiniteDuration] = None
) {

  def withBatchSize(batchSize: Int): AzureQueueSourceSettings =
    copy(batchSize = batchSize)

  /**
   * @param retrieveRetryTimeout in seconds. If <= 0 retrying of message retrieval is disabled.
   * @return
   */
  def withRetrieveRetryTimeout(retrieveRetryTimeout: FiniteDuration): AzureQueueSourceSettings =
    copy(retrieveRetryTimeout = Some(retrieveRetryTimeout))

  def withRetrieveRetryTimeout(retrieveRetryTimeout: JavaDuration) =
    copy(retrieveRetryTimeout = Some(Duration.fromNanos(retrieveRetryTimeout.toNanos)))

  /**
   * Java API
   */
  def getRetrieveRetryTimeout(): Optional[JavaDuration] =
    retrieveRetryTimeout.map(d => JavaDuration.ofNanos(d.toNanos)).asJava

  private def copy(batchSize: Int = batchSize, retrieveRetryTimeout: Option[FiniteDuration] = retrieveRetryTimeout) =
    new AzureQueueSourceSettings(initialVisibilityTimeout, batchSize, retrieveRetryTimeout)

  override def toString: String =
    s"AzureQueueSourceSettings(initialVisibilityTimeout=$initialVisibilityTimeout, batchSize=$batchSize, retrieveRetryTimeout=$retrieveRetryTimeout)"
}

object AzureQueueSourceSettings {

  def apply(initialVisibilityTimeout: Int, batchSize: Int): AzureQueueSourceSettings =
    new AzureQueueSourceSettings(initialVisibilityTimeout, batchSize)

  def create(initialVisibilityTimeout: Int, batchSize: Int): AzureQueueSourceSettings =
    AzureQueueSourceSettings(initialVisibilityTimeout, batchSize)

  /**
   * Default settings
   *
   * initialVisibilityTimeout (30) is taken from
   * [[com.microsoft.azure.storage.queue.QueueConstants.DEFAULT_VISIBILITY_MESSAGE_TIMEOUT_IN_SECONDS]]
   */
  def apply(): AzureQueueSourceSettings = AzureQueueSourceSettings(30, 10)

  /**
   * Java API
   *
   * Default settings
   *
   * initialVisibilityTimeout (30) is taken from
   * [[com.microsoft.azure.storage.queue.QueueConstants.DEFAULT_VISIBILITY_MESSAGE_TIMEOUT_IN_SECONDS]]
   */
  def create(): AzureQueueSourceSettings = AzureQueueSourceSettings()
}
