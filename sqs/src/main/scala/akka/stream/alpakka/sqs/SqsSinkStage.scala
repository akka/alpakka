/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import akka.Done
import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, StageLogging}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}

import scala.concurrent.{Future, Promise}

object SqsSinkSettings {
  val Defaults = SqsSinkSettings(maxInFlight = 10)
}

//#SqsSinkSettings
final case class SqsSinkSettings(maxInFlight: Int) {
  require(maxInFlight > 0)
}
//#SqsSinkSettings

class SqsSinkStage(queueUrl: String, settings: SqsSinkSettings, sqsClient: AmazonSQSAsync)
    extends GraphStageWithMaterializedValue[SinkShape[String], Future[Done]] {

  val in: Inlet[String] = Inlet("SqsSink.in")

  override val shape: SinkShape[String] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new SqsSinkStageLogic(queueUrl, settings, sqsClient, in, shape, promise)

    (logic, promise.future)
  }
}

private[sqs] class SqsSinkStageLogic(
    queueUrl: String,
    settings: SqsSinkSettings,
    sqsClient: AmazonSQSAsync,
    in: Inlet[String],
    shape: SinkShape[String],
    promise: Promise[Done]
) extends GraphStageLogic(shape)
    with StageLogging {

  private var inFlight = 0
  private var isShutdownInProgress = false
  private var amazonSendMessageHandler: AsyncHandler[SendMessageRequest, SendMessageResult] = _

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        inFlight += 1
        sqsClient.sendMessageAsync(new SendMessageRequest(queueUrl, grab(in)), amazonSendMessageHandler)
        tryPull()
      }

      override def onUpstreamFailure(exception: Throwable): Unit = {
        log.error(exception, "Upstream failure: {}", exception.getMessage)
        failStage(exception)
        promise.tryFailure(exception)
      }

      override def onUpstreamFinish(): Unit = {
        isShutdownInProgress = true
        tryShutdown()
      }
    }
  )

  override def preStart(): Unit = {
    setKeepGoing(true)

    val failureCallback = getAsyncCallback[Throwable](handleFailure)
    val sendCallback = getAsyncCallback[SendMessageResult](handleResult)

    amazonSendMessageHandler = new AsyncHandler[SendMessageRequest, SendMessageResult] {
      override def onError(exception: Exception): Unit =
        failureCallback.invoke(exception)

      override def onSuccess(request: SendMessageRequest, result: SendMessageResult): Unit =
        sendCallback.invoke(result)
    }

    // This requests one element at the Sink startup.
    pull(in)
  }

  private def handleResult(result: SendMessageResult): Unit = {
    log.debug(s"Sent SQS message {}", result.getMessageId)
    inFlight -= 1
    tryShutdown()
    tryPull()
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
    log.error(exception, "AmazonSQSAsync failure: {}", exception.getMessage)
    inFlight -= 1
    failStage(exception)
    promise.tryFailure(exception)
  }
}
