/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import akka.Done
import akka.stream._
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler, StageLogging }
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.concurrent.{ Future, Promise }

object SqsAckSinkSettings {
  val Defaults = SqsAckSinkSettings(maxInFlight = 10)
}

//#SqsAckSinkSettings
final case class SqsAckSinkSettings(maxInFlight: Int) {
  require(maxInFlight > 0)
}
//#SqsAckSinkSettings

sealed trait MessageAction
final case class Ack() extends MessageAction
final case class RequeueWithDelay(delaySeconds: Int) extends MessageAction

class SqsAckSinkStage(queueUrl: String, settings: SqsAckSinkSettings, sqsClient: AmazonSQSAsync)
    extends GraphStageWithMaterializedValue[SinkShape[MessageActionPair], Future[Done]] {

  val in: Inlet[MessageActionPair] = Inlet("SqsAckSink.in")

  override val shape: SinkShape[MessageActionPair] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new SqsAckSinkStageLogic(queueUrl, settings, sqsClient, in, shape, promise)

    (logic, promise.future)
  }
}

private[sqs] class SqsAckSinkStageLogic(
    queueUrl: String,
    settings: SqsAckSinkSettings,
    sqsClient: AmazonSQSAsync,
    in: Inlet[MessageActionPair],
    shape: SinkShape[MessageActionPair],
    promise: Promise[Done]
) extends GraphStageLogic(shape)
    with StageLogging {

  private var inFlight = 0
  private var isShutdownInProgress = false
  private var amazonSendMessageHandler: AsyncHandler[SendMessageRequest, SendMessageResult] = _
  private var amazonDeleteMessageHandler: AsyncHandler[DeleteMessageRequest, DeleteMessageResult] = _

  setHandler(in,
    new InHandler {
    override def onPush(): Unit = {
      inFlight += 1
      val (message, action) = grab(in)
      action match {
        case Ack() =>
          sqsClient.deleteMessageAsync(
            new DeleteMessageRequest(queueUrl, message.getReceiptHandle),
            amazonDeleteMessageHandler
          )
        case RequeueWithDelay(delaySeconds) =>
          sqsClient.sendMessageAsync(
            new SendMessageRequest(queueUrl, message.getBody).withDelaySeconds(delaySeconds),
            amazonSendMessageHandler
          )
      }

      tryPull()
    }

    override def onUpstreamFailure(exception: Throwable): Unit = {
      log.error(exception, "Upstream failure: {}", exception.getMessage)
      failStage(exception)
      promise.tryFailure(exception)
    }

    override def onUpstreamFinish(): Unit = {
      log.debug("Upstream finish")
      isShutdownInProgress = true
      tryShutdown()
    }
  })

  override def preStart(): Unit = {
    setKeepGoing(true)

    val failureCallback = getAsyncCallback[Throwable](handleFailure)
    val sendCallback = getAsyncCallback[SendMessageResult](handleSend)
    val deleteCallback = getAsyncCallback[DeleteMessageRequest](handleDelete)

    amazonSendMessageHandler = new AsyncHandler[SendMessageRequest, SendMessageResult] {
      override def onError(exception: Exception): Unit =
        failureCallback.invoke(exception)

      override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit =
        sendCallback.invoke(result)
    }

    amazonDeleteMessageHandler = new AsyncHandler[DeleteMessageRequest, DeleteMessageResult] {
      override def onError(exception: Exception): Unit =
        failureCallback.invoke(exception)

      override def onSuccess(request: DeleteMessageRequest, result: DeleteMessageResult): Unit =
        deleteCallback.invoke(request)
    }

    pull(in)
  }

  private def tryPull(): Unit =
    if (inFlight < settings.maxInFlight && !isClosed(in) && !hasBeenPulled(in)) {
      pull(in)
    }

  private def tryShutdown(): Unit =
    if (isShutdownInProgress && inFlight <= 0) {
      completeStage()
      promise.trySuccess(Done)
    }

  private def handleFailure(exception: Throwable): Unit = {
    log.error(exception, "Client failure: {}", exception.getMessage)
    inFlight -= 1
    failStage(exception)
    promise.tryFailure(exception)
  }

  private def handleSend(result: SendMessageResult): Unit = {
    log.debug(s"Sent message {}", result.getMessageId)
    inFlight -= 1
    tryShutdown()
    tryPull()
  }

  private def handleDelete(request: DeleteMessageRequest): Unit = {
    log.debug(s"Deleted message {}", request.getReceiptHandle)
    inFlight -= 1
    tryShutdown()
    tryPull()
  }
}
