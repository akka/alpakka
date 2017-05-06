/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.scaladsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.{AzureQueueSinkFunctions, DeleteOrUpdateMessage, FlowMapECStage}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.Done
import scala.concurrent.{ExecutionContext, Future}

object AzureQueueSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which queues message to an Azure Storage Queue.
   */
  def apply(cloudQueue: CloudQueue, maxInFlight: Int = 4): Sink[CloudQueueMessage, Future[Done]] =
    fromFunction(AzureQueueSinkFunctions.addMessage(cloudQueue)(_)(_), maxInFlight)

  def fromFunction[T](f: (T, ExecutionContext) => Future[Done], maxInFlight: Int): Sink[T, Future[Done]] = {
    val flowStage = new FlowMapECStage[T, Future[Done]](f)
    Flow
      .fromGraph(flowStage)
      .mapAsync(maxInFlight)(identity)
      .toMat(Sink.ignore)(Keep.right)
  }
}

object AzureQueueWithTimeoutsSink {

  /**
   * ScalaAPI: creates an [[akka.stream.scaladsl.Sink]] with queues message to an Azure Storage Queue.
   * This is the same as [[AzureQueueSink.apply]] expect that the sink takes instead
   * of a [[com.microsoft.azure.storage.queue.CouldQueueMessage]] a tuple
   * with (CouldQueueMessage, timeToLive, initialVisibilityTimeout).
   */
  def apply(cloudQueue: CloudQueue, maxInFlight: Int = 4): Sink[(CloudQueueMessage, Int, Int), Future[Done]] =
    AzureQueueSink.fromFunction(
      (tup, ec) => AzureQueueSinkFunctions.addMessage(cloudQueue)(tup._1, tup._2, tup._3)(ec),
      maxInFlight
    )
}

object AzureQueueDeleteSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which deletes messages from an Azure Storage Queue.
   */
  def apply(cloudQueue: CloudQueue, maxInFlight: Int = 4): Sink[CloudQueueMessage, Future[Done]] =
    AzureQueueSink.fromFunction(AzureQueueSinkFunctions.deleteMessage(cloudQueue)(_)(_), maxInFlight)
}

object AzureQueueDeleteOrUpdateSink {

  /**
   * ScalaAPI: creates a [[akka.stream.scaladsl.Sink]] which deletes or updates the visibility timeout of messages
   * in an Azure Storage Queue.
   */
  def apply(cloudQueue: CloudQueue,
            maxInFlight: Int = 4): Sink[(CloudQueueMessage, DeleteOrUpdateMessage), Future[Done]] =
    AzureQueueSink.fromFunction(
      (input, ec) => AzureQueueSinkFunctions.deleteOrUpdateMessage(cloudQueue)(input._1, input._2)(ec),
      maxInFlight
    )
}
