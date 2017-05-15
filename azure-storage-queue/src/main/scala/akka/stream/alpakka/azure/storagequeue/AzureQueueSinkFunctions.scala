/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}

sealed trait DeleteOrUpdateMessage
case object Delete extends DeleteOrUpdateMessage
case class UpdateVisibility(timeout: Int) extends DeleteOrUpdateMessage

private[storagequeue] object AzureQueueSinkFunctions {
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
      case Delete => deleteMessage(cloudQueue)(msg)
      case UpdateVisibility(timeout) => updateMessage(cloudQueue)(msg, timeout)
    }
}
