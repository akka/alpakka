/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import scala.concurrent.{ExecutionContext, Future}
import akka.Done

sealed trait DeleteOrUpdateMessage
case object Delete extends DeleteOrUpdateMessage
case class UpdateVisibility(timeout: Int) extends DeleteOrUpdateMessage

private[storagequeue] object AzureQueueSinkFunctions {
  def addMessage(
      cloudQueue: CloudQueue
  )(msg: CloudQueueMessage, timeToLive: Int = 0, initialVisibilityTimeout: Int = 0)(
      implicit executionContext: ExecutionContext
  ): Future[Done] =
    Future {
      cloudQueue.addMessage(msg, timeToLive, initialVisibilityTimeout, null, null)
      Done
    }

  def deleteMessage(
      cloudQueue: CloudQueue
  )(msg: CloudQueueMessage)(implicit executionContext: ExecutionContext): Future[Done] =
    Future {
      cloudQueue.deleteMessage(msg)
      Done
    }

  def updateMessage(cloudQueue: CloudQueue)(msg: CloudQueueMessage,
                                            timeout: Int)(implicit executionContext: ExecutionContext): Future[Done] =
    Future {
      cloudQueue.updateMessage(msg, timeout)
      Done
    }

  def deleteOrUpdateMessage(
      cloudQueue: CloudQueue
  )(msg: CloudQueueMessage, op: DeleteOrUpdateMessage)(implicit executionContext: ExecutionContext): Future[Done] =
    op match {
      case Delete => deleteMessage(cloudQueue)(msg)
      case UpdateVisibility(timeout) => updateMessage(cloudQueue)(msg, timeout)
    }
}
