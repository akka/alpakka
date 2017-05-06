/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.javadsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.{
  AzureQueueSinkFunctions,
  Delete,
  DeleteOrUpdateMessage,
  FlowMapECStage,
  UpdateVisibility
}
import akka.stream.javadsl.Sink
import akka.Done
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.CompletionStage

object AzureQueueSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which queues message to an Azure Storage Queue.
   */
  def create(cloudQueue: CloudQueue, maxInFlight: Int): Sink[CloudQueueMessage, CompletionStage[Done]] =
    fromFunction(AzureQueueSinkFunctions.addMessage(cloudQueue)(_)(_), maxInFlight)

  def create(cloudQueue: CloudQueue): Sink[CloudQueueMessage, CompletionStage[Done]] =
    create(cloudQueue, 4)

  private[javadsl] def fromFunction[T](f: (T, ExecutionContext) => Future[Done],
                                       maxInFlight: Int): Sink[T, CompletionStage[Done]] = {
    import akka.stream.alpakka.azure.storagequeue.scaladsl.{AzureQueueSink => AzureQueueSinkScalaDSL}
    import scala.compat.java8.FutureConverters._
    AzureQueueSinkScalaDSL.fromFunction(f, maxInFlight).mapMaterializedValue(_.toJava).asJava
  }
}

class MessageWithTimeouts(val message: CloudQueueMessage, val timeToLive: Int, val initialVisibility: Int)

object AzureQueueWithTimeoutsSink {

  /**
   * JavaAPI: creates an [[akka.stream.javadsl.Sink]] with queues message to an Azure Storage Queue.
   * This is the same as [[AzureQueueSink.create]] expect that it takes instead
   * of a [[com.microsoft.azure.storage.queue.CouldQueueMessage]] a [[MessageWithTimeouts]].
   */
  def create(cloudQueue: CloudQueue, maxInFlight: Int): Sink[MessageWithTimeouts, CompletionStage[Done]] =
    AzureQueueSink.fromFunction(
      (input: MessageWithTimeouts, ec: ExecutionContext) =>
        AzureQueueSinkFunctions.addMessage(cloudQueue)(input.message, input.timeToLive, input.initialVisibility)(ec),
      maxInFlight
    )

  def create(cloudQueue: CloudQueue): Sink[MessageWithTimeouts, CompletionStage[Done]] =
    create(cloudQueue, 4)
}

object AzureQueueDeleteSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which deletes messages from an Azure Storage Queue.
   */
  def create(cloudQueue: CloudQueue, maxInFlight: Int): Sink[CloudQueueMessage, CompletionStage[Done]] =
    AzureQueueSink.fromFunction(AzureQueueSinkFunctions.deleteMessage(cloudQueue)(_)(_), maxInFlight)

  def create(cloudQueue: CloudQueue): Sink[CloudQueueMessage, CompletionStage[Done]] =
    create(cloudQueue, 4)
}

class MessageAndDeleteOrUpdate(val message: CloudQueueMessage, val op: DeleteOrUpdateMessage)
object MessageAndDeleteOrUpdate {
  def delete = Delete
  def updateVisibility(timeout: Integer) = UpdateVisibility(timeout)
}

object AzureQueueDeleteOrUpdateSink {

  /**
   * JavaAPI: creates a [[akka.stream.javadsl.Sink]] which deletes or updates the visibility timeout of messages
   * in an Azure Storage Queue.
   */
  def create(cloudQueue: CloudQueue, maxInFlight: Int): Sink[MessageAndDeleteOrUpdate, CompletionStage[Done]] =
    AzureQueueSink.fromFunction(
      (input, ec) => AzureQueueSinkFunctions.deleteOrUpdateMessage(cloudQueue)(input.message, input.op)(ec),
      maxInFlight
    )
  def create(cloudQueue: CloudQueue): Sink[MessageAndDeleteOrUpdate, CompletionStage[Done]] =
    create(cloudQueue, 4)
}
