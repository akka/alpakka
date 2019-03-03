/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.azure.storagequeue.javadsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.impl.AzureQueueSinkFunctions
import akka.stream.javadsl.Sink
import akka.Done
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import akka.stream.alpakka.azure.storagequeue.DeleteOrUpdateMessage

object AzureQueueSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which queues message to an Azure Storage Queue.
   */
  def create(cloudQueue: Supplier[CloudQueue]): Sink[CloudQueueMessage, CompletionStage[Done]] =
    fromFunction(AzureQueueSinkFunctions.addMessage(() => cloudQueue.get)(_))

  /**
   * Internal API
   */
  private[javadsl] def fromFunction[T](f: T => Unit): Sink[T, CompletionStage[Done]] = {
    import akka.stream.alpakka.azure.storagequeue.scaladsl.{AzureQueueSink => AzureQueueSinkScalaDSL}
    import scala.compat.java8.FutureConverters._
    AzureQueueSinkScalaDSL.fromFunction(f).mapMaterializedValue(_.toJava).asJava
  }
}

class MessageWithTimeouts(val message: CloudQueueMessage, val timeToLive: Int, val initialVisibility: Int)

object AzureQueueWithTimeoutsSink {

  /**
   * JavaAPI: creates an [[akka.stream.javadsl.Sink]] with queues message to an Azure Storage Queue.
   * This is the same as [[AzureQueueSink.create]] expect that it takes instead
   * of a [[com.microsoft.azure.storage.queue.CouldQueueMessage]] a [[MessageWithTimeouts]].
   */
  def create(cloudQueue: Supplier[CloudQueue]): Sink[MessageWithTimeouts, CompletionStage[Done]] =
    AzureQueueSink.fromFunction(
      { input: MessageWithTimeouts =>
        AzureQueueSinkFunctions
          .addMessage(() => cloudQueue.get)(input.message, input.timeToLive, input.initialVisibility)
      }
    )
}

object AzureQueueDeleteSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which deletes messages from an Azure Storage Queue.
   */
  def create(cloudQueue: Supplier[CloudQueue]): Sink[CloudQueueMessage, CompletionStage[Done]] =
    AzureQueueSink.fromFunction[CloudQueueMessage](AzureQueueSinkFunctions.deleteMessage(() => cloudQueue.get)(_))
}

class MessageAndDeleteOrUpdate(val message: CloudQueueMessage, val op: DeleteOrUpdateMessage)

object AzureQueueDeleteOrUpdateSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which deletes or updates the visibility timeout of messages
   * in an Azure Storage Queue.
   */
  def create(cloudQueue: Supplier[CloudQueue]): Sink[MessageAndDeleteOrUpdate, CompletionStage[Done]] =
    AzureQueueSink.fromFunction[MessageAndDeleteOrUpdate](
      input => AzureQueueSinkFunctions.deleteOrUpdateMessage(() => cloudQueue.get)(input.message, input.op)
    )
}
