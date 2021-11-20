/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.azure.storagequeue.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.azure.storagequeue.DeleteOrUpdateMessage
import akka.stream.alpakka.azure.storagequeue.DeleteOrUpdateMessage.{Delete, UpdateVisibility}
import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}

/**
 * INTERNAL API
 */
@InternalApi private[storagequeue] object AzureQueueSinkFunctions {
  def addMessage(
      cloudQueue: () => CloudQueue
  )(msg: CloudQueueMessage, timeToLive: Int = 0, initialVisibilityTimeout: Int = 0): Unit =
    cloudQueue().addMessage(msg, timeToLive, initialVisibilityTimeout, null, null)

  def deleteMessage(
      cloudQueue: () => CloudQueue
  )(msg: CloudQueueMessage): Unit =
    cloudQueue().deleteMessage(msg)

  def updateMessage(cloudQueue: () => CloudQueue)(msg: CloudQueueMessage, timeout: Int): Unit =
    cloudQueue().updateMessage(msg, timeout)

  def deleteOrUpdateMessage(
      cloudQueue: () => CloudQueue
  )(msg: CloudQueueMessage, op: DeleteOrUpdateMessage): Unit =
    op match {
      case _: Delete => deleteMessage(cloudQueue)(msg)
      case m: UpdateVisibility => updateMessage(cloudQueue)(msg, m.timeout)
    }
}
