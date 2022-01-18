/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.azure.storagequeue.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.azure.storagequeue.AzureQueueSourceSettings
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}

import scala.collection.mutable.Queue

/**
 * INTERNAL API
 */
@InternalApi private[storagequeue] final class AzureQueueSourceStage(cloudQueue: () => CloudQueue,
                                                                     settings: AzureQueueSourceSettings)
    extends GraphStage[SourceShape[CloudQueueMessage]] {
  val out: Outlet[CloudQueueMessage] = Outlet("AzureCloudQueue.out")
  override val shape: SourceShape[CloudQueueMessage] = SourceShape(out)

  override def initialAttributes: Attributes =
    super.initialAttributes and IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    private val buffer = new Queue[CloudQueueMessage]

    lazy val cloudQueueBuilt = cloudQueue()

    override def onTimer(timerKey: Any): Unit =
      retrieveMessages()

    def retrieveMessages(): Unit = {
      import scala.jdk.CollectionConverters._
      val res = cloudQueueBuilt
        .retrieveMessages(settings.batchSize, settings.initialVisibilityTimeout, null, null)
        .asScala
        .toList

      if (res.isEmpty) {
        settings.retrieveRetryTimeout match {
          case Some(timeout) =>
            if (isAvailable(out)) {
              scheduleOnce(NotUsed, timeout)
            }
          case None => complete(out)
        }
      } else {
        buffer ++= res
        push(out, buffer.dequeue())
      }
    }

    setHandler(
      out,
      new OutHandler {
        override def onPull: Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.dequeue())
          } else {
            retrieveMessages()
          }
      }
    )
  }
}
