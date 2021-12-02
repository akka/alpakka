/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.azure.storagequeue.scaladsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.impl.AzureQueueSinkFunctions
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.Done

import scala.concurrent.Future
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.Attributes
import akka.stream.alpakka.azure.storagequeue.DeleteOrUpdateMessage

object AzureQueueSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which queues message to an Azure Storage Queue.
   */
  def apply(cloudQueue: () => CloudQueue): Sink[CloudQueueMessage, Future[Done]] =
    fromFunction(AzureQueueSinkFunctions.addMessage(cloudQueue)(_))

  /**
   * Internal API
   */
  def fromFunction[T](f: T => Unit): Sink[T, Future[Done]] =
    Flow
      .fromFunction(f)
      .addAttributes(Attributes(IODispatcher))
      .toMat(Sink.ignore)(Keep.right)
}

object AzureQueueWithTimeoutsSink {

  /**
   * ScalaAPI: creates an [[akka.stream.scaladsl.Sink]] with queues message to an Azure Storage Queue.
   * This is the same as [[AzureQueueSink.apply]] expect that the sink takes instead
   * of a [[com.microsoft.azure.storage.queue.CouldQueueMessage]] a tuple
   * with (CouldQueueMessage, timeToLive, initialVisibilityTimeout).
   */
  def apply(
      cloudQueue: () => CloudQueue
  ): Sink[(CloudQueueMessage, Int, Int), Future[Done]] =
    AzureQueueSink.fromFunction(
      tup => AzureQueueSinkFunctions.addMessage(cloudQueue)(tup._1, tup._2, tup._3)
    )
}

object AzureQueueDeleteSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which deletes messages from an Azure Storage Queue.
   */
  def apply(cloudQueue: () => CloudQueue): Sink[CloudQueueMessage, Future[Done]] =
    AzureQueueSink.fromFunction(AzureQueueSinkFunctions.deleteMessage(cloudQueue)(_))
}

object AzureQueueDeleteOrUpdateSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which deletes or updates the visibility timeout of messages
   * in an Azure Storage Queue.
   */
  def apply(
      cloudQueue: () => CloudQueue
  ): Sink[(CloudQueueMessage, DeleteOrUpdateMessage), Future[Done]] =
    AzureQueueSink.fromFunction(
      input => AzureQueueSinkFunctions.deleteOrUpdateMessage(cloudQueue)(input._1, input._2)
    )
}
