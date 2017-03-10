/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}

import scala.concurrent.{Future, Promise}

final class SnsPublishSinkStage(topicArn: String, snsClient: AmazonSNSAsync)
    extends GraphStageWithMaterializedValue[SinkShape[String], Future[Done]] {

  private val in = Inlet[String]("SnsPublishSink.in")

  override def shape: SinkShape[String] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val completed = Promise[Done]()

    val logic = new GraphStageLogic(shape) with InHandler with StageLogging {

      private def handleFailure(ex: Throwable): Unit = {
        failStage(ex)
        completed.tryFailure(ex)
      }

      private def handleSuccess(result: PublishResult): Unit = {
        log.debug("Published SNS message: {}", result.getMessageId)
        completed.trySuccess(Done)

        if (!hasBeenPulled(in)) tryPull(in)
      }

      private val failureCallback = getAsyncCallback[Throwable](handleFailure)
      private val successCallback = getAsyncCallback[PublishResult](handleSuccess)

      override def onPush(): Unit = {
        val request = new PublishRequest().withTopicArn(topicArn).withMessage(grab(in))

        snsClient.publishAsync(request,
          new AsyncHandler[PublishRequest, PublishResult] {
          override def onError(exception: Exception): Unit =
            failureCallback.invoke(exception)

          override def onSuccess(request: PublishRequest, result: PublishResult): Unit =
            successCallback.invoke(result)
        })
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
        completed.trySuccess(Done)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        failStage(ex)
        completed.tryFailure(ex)
      }

      override def preStart(): Unit = {
        setKeepGoing(true)
        pull(in)
      }

      setHandler(in, this)
    }

    (logic, completed.future)
  }
}
