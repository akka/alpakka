/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.ironmq.IronMqSettings.ConsumerSettings
import akka.stream.alpakka.ironmq._
import akka.stream.alpakka.ironmq.scaladsl.CommittableMessage
import akka.stream.stage._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

@InternalApi
private[ironmq] object IronMqPullStage {

  private val FetchMessagesTimerKey = "fetch-messages"
}

/**
 * Internal API.
 *
 * This stage will fetch messages from IronMq and buffer them internally.
 *
 * It is implemented as a timed loop, each invocation will fetch new messages from IronMq if the amount of buffered
 * messages is lower than [[akka.stream.alpakka.ironmq.IronMqSettings.ConsumerSettings.bufferMinSize]].
 *
 * The frequency of the loop is controlled by [[akka.stream.alpakka.ironmq.IronMqSettings.ConsumerSettings.fetchInterval]] while the amount of time the client is
 * blocked on the HTTP request waiting for messages is controlled by [[ConsumerSettings.pollTimeout]].
 *
 * Keep in mind that the IronMq time unit is the second, so any value below the second is considered 0.
 */
@InternalApi
private[ironmq] final class IronMqPullStage(queueName: String, settings: IronMqSettings)
    extends GraphStage[SourceShape[CommittableMessage]] {

  import IronMqPullStage._

  val consumerSettings: ConsumerSettings = settings.consumerSettings
  import consumerSettings._

  private val out: Outlet[CommittableMessage] = Outlet("IronMqPull.out")

  override protected val initialAttributes: Attributes = Attributes.name("IronMqPull")

  override val shape: SourceShape[CommittableMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {

      implicit def ec: ExecutionContextExecutor = materializer.executionContext

      // This flag avoid run concurrent fetch from IronMQ
      private var fetching: Boolean = false

      private var buffer: List[ReservedMessage] = List.empty
      private var client: IronMqClient = _ // set in preStart

      override def preStart(): Unit =
        client = IronMqClient(settings)(materializer.system, materializer)

      setHandler(
        out,
        new OutHandler {

          override def onPull(): Unit = {
            if (!isTimerActive(FetchMessagesTimerKey)) {
              scheduleAtFixedRate(FetchMessagesTimerKey, fetchInterval, fetchInterval)
            }
            deliveryMessages()
          }
        }
      )

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case FetchMessagesTimerKey =>
          fetchMessages()
      }

      def fetchMessages(): Unit =
        if (!fetching && buffer.size < bufferMinSize) {
          fetching = true
          client
            .reserveMessages(
              queueName,
              bufferMaxSize - buffer.size,
              watch = pollTimeout,
              timeout = reservationTimeout
            )
            .onComplete {
              case Success(xs) =>
                updateBuffer.invoke(xs.toList)
                updateFetching.invoke(false)
              case Failure(error) =>
                fail(out, error)
                updateFetching.invoke(false)
            }
        }

      def deliveryMessages(): Unit =
        while (buffer.nonEmpty && isAvailable(out)) {
          val messageToDelivery: ReservedMessage = buffer.head

          val committableMessage: CommittableMessage = new CommittableMessage {
            override val message: Message = messageToDelivery.message
            override def commit(): Future[Done] =
              client.deleteMessages(queueName, messageToDelivery.reservation).map(_ => Done)
          }

          push(out, committableMessage)
          buffer = buffer.tail
        }

      private val updateBuffer = getAsyncCallback { xs: List[ReservedMessage] =>
        buffer = buffer ::: xs
        deliveryMessages()
      }

      private val updateFetching = getAsyncCallback { x: Boolean =>
        fetching = x
      }
    }

}
