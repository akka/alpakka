/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.util.{Failure, Success}

final case class AzureQueueSourceSettings(
    waitTimeSeconds: Int,
    maxBufferSize: Int,
    maxBatchSize: Int
) {
  require(maxBatchSize <= maxBufferSize, "maxBatchSize must be lower or equal than maxBufferSize")
}

object AzureQueueSourceSettings {
  val default = AzureQueueSourceSettings(20, 100, 10)

  /**
   *  Java API
   */
  def create(waitTimeSeconds: Int, maxBufferSize: Int, maxBatchSize: Int): AzureQueueSourceSettings =
    AzureQueueSourceSettings(waitTimeSeconds, maxBufferSize, maxBatchSize)
}

private[storagequeue] final class AzureQueueSourceStage(cloudQueue: CloudQueue, settings: AzureQueueSourceSettings)
    extends GraphStage[SourceShape[CloudQueueMessage]] {
  val out: Outlet[CloudQueueMessage] = Outlet("AzureCloudQueue.out")
  override val shape: SourceShape[CloudQueueMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val buffer = new Queue[CloudQueueMessage]

    private val maxConcurrency = settings.maxBufferSize / settings.maxBatchSize
    private var maxCurrentConcurrency = maxConcurrency
    private var currentRequests = 0

    private val successCallback = getAsyncCallback(handleSuccess)
    private val failureCallback = getAsyncCallback(handleFailure)

    private def canReceiveNewMessages: Boolean = {
      val currentFreeRequests = (settings.maxBufferSize - buffer.size) / settings.maxBatchSize
      currentFreeRequests > currentRequests && maxCurrentConcurrency > currentRequests
    }

    def receiveMessages(): Unit =
      if (canReceiveNewMessages) {
        implicit val executionContext = materializer.executionContext
        currentRequests += 1

        Future {
          import scala.collection.JavaConverters._
          val res = cloudQueue.retrieveMessages(settings.maxBatchSize, settings.waitTimeSeconds, null, null)
          res.asScala.toList
        } onComplete {
          case Success(res) => successCallback.invoke(res)
          case Failure(t) => failureCallback.invoke(t)
        }

      }

    def handleFailure(ex: Throwable): Unit = failStage(ex)

    def handleSuccess(result: List[CloudQueueMessage]): Unit = {
      currentRequests -= 1
      maxCurrentConcurrency = if (result.isEmpty) 1 else maxConcurrency
      buffer ++= result

      if (!buffer.isEmpty && isAvailable(out)) {
        push(out, buffer.dequeue)
      }
      receiveMessages
    }

    setHandler(out, new OutHandler {
      override def onPull: Unit = {
        if (!buffer.isEmpty) {
          push(out, buffer.dequeue)
        }
        receiveMessages
      }
    })
  }
}
