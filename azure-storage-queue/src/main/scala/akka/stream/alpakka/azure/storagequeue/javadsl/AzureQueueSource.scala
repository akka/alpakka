/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.javadsl

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.alpakka.azure.storagequeue.{AzureQueueSourceSettings, AzureQueueSourceStage}
import akka.stream.javadsl.Source
import akka.NotUsed

object AzureQueueSource {

  /**
   * Java API: creates a [[AzureQueueSource]] for a Azure CloudQueue.
   */
  def create(cloudQueue: CloudQueue, settings: AzureQueueSourceSettings): Source[CloudQueueMessage, NotUsed] =
    Source.fromGraph(new AzureQueueSourceStage(cloudQueue, settings))

  def create(cloudQueue: CloudQueue): Source[CloudQueueMessage, NotUsed] =
    create(cloudQueue, AzureQueueSourceSettings.default)
}
