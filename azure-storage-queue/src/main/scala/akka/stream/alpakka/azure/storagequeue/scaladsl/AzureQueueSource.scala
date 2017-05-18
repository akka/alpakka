/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.scaladsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.{AzureQueueSourceSettings, AzureQueueSourceStage}
import akka.stream.scaladsl.Source
import akka.NotUsed

object AzureQueueSource {

  /**
   * Scala API: creates a [[AzureQueueSource]] for a Azure CloudQueue.
   */
  def apply(
      cloudQueue: () => CloudQueue,
      settings: AzureQueueSourceSettings = AzureQueueSourceSettings.Default
  ): Source[CloudQueueMessage, NotUsed] =
    Source.fromGraph(new AzureQueueSourceStage(cloudQueue, settings))
}
