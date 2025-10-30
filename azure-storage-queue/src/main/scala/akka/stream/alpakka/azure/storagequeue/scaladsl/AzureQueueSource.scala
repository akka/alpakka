/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.azure.storagequeue.scaladsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.AzureQueueSourceSettings
import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.stream.alpakka.azure.storagequeue.impl.AzureQueueSourceStage

object AzureQueueSource {

  /**
   * Scala API: creates a [[AzureQueueSource]] for a Azure CloudQueue.
   */
  def apply(
      cloudQueue: () => CloudQueue,
      settings: AzureQueueSourceSettings = AzureQueueSourceSettings()
  ): Source[CloudQueueMessage, NotUsed] =
    Source.fromGraph(new AzureQueueSourceStage(cloudQueue, settings))
}
