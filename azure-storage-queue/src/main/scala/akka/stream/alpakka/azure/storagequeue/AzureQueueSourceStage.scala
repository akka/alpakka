/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue

import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueMessage}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.actor.ActorSystem
import scala.collection.mutable.Queue
import scala.concurrent.duration._
import akka.NotUsed
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher

/** Settings for AzureQueueSource
 *
 * @param initalVisibilityTimeout Specifies how many seconds a message becomes invisible after it has been dequeued.
 *        See parameter of the same name in [[com.microsoft.azure.storage.queue.CloudQueue$.retrieveMessages]].
 * @param batchSize Specifies how many message are fetched in one batch.
 *        (This is the numberOfMessages parameter in [[com.microsoft.azure.storage.queue.CloudQueue$.retrieveMessages]].)
 * @param retrieveRetryTimeout If None the [[AzureQueueSource]] will be completed if the queue is empty.
 *        If Some(timeout) [[AzureQueueSource]] will retry after timeout to get new messages. Do not set timeout to low.
 */
final case class AzureQueueSourceSettings(
    initialVisibilityTimeout: Int,
    batchSize: Int,
    retrieveRetryTimeout: Option[FiniteDuration]
)

object AzureQueueSourceSettings {

  /** Default settings
   *
   * initialVisibilityTimeout (30) is taken from
   * [[com.microsoft.azure.storage.queue.QueueConstants.DEFAULT_VISIBILITY_MESSAGE_TIMEOUT_IN_SECONDS]]
   */
  val Default = AzureQueueSourceSettings(30, 10, None)

  /**
   *  Java API: Constructor for [[AzureQueueSourceSettings]]
   *  @param retrieveRetryTimeout in seconds. If <= 0 retrying of message retrieval is disable.
   */
  def create(initialVisibilityTimeout: Int, batchSize: Int, retrieveRetryTimeout: Int): AzureQueueSourceSettings = {
    val rrTimeout = if (retrieveRetryTimeout > 0) Some(retrieveRetryTimeout.seconds) else None
    AzureQueueSourceSettings(initialVisibilityTimeout, batchSize, rrTimeout)
  }
}

private[storagequeue] final class AzureQueueSourceStage(cloudQueue: () => CloudQueue,
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
      import scala.collection.JavaConverters._
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
        push(out, buffer.dequeue)
      }
    }

    setHandler(
      out,
      new OutHandler {
        override def onPull: Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.dequeue)
          } else {
            retrieveMessages()
          }
      }
    )
  }
}
