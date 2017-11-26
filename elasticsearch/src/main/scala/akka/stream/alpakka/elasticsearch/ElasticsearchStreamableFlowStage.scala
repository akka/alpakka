/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import java.nio.charset.StandardCharsets

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}

import scala.collection.mutable
import scala.collection.JavaConverters._
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import ElasticsearchStreamableFlowStage._
import akka.NotUsed
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSinkSettings
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import scala.collection.JavaConverters._
import java.util.{List => JavaList}

trait StreamableIncomingMessage {
  def id: Option[String]
  def json: String
}

final case class StreamableIncomingMessagesResult[T <: StreamableIncomingMessage](
    success: Seq[T],
    failed: Seq[T]
) {
  // Java-API
  def getSuccessList(): JavaList[T] = success.toList.asJava
  // Java-API
  def getFailedList(): JavaList[T] = failed.toList.asJava
}

class ElasticsearchStreamableFlowStage[T <: StreamableIncomingMessage](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchSinkSettings
) extends GraphStage[FlowShape[T, Future[StreamableIncomingMessagesResult[T]]]] {

  private val in = Inlet[T]("messages")
  private val out = Outlet[Future[StreamableIncomingMessagesResult[T]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private var state: State = Idle
      private val queue = new mutable.Queue[T]()
      private val failureHandler = getAsyncCallback[(Seq[T], Throwable)](handleFailure)
      private val responseHandler = getAsyncCallback[(Seq[T], Response)](handleResponse)
      private var failedMessages: Seq[T] = Nil
      private var retryCount: Int = 0

      override def preStart(): Unit =
        pull(in)

      private def tryPull(): Unit =
        if (queue.size < settings.bufferSize && !isClosed(in) && !hasBeenPulled(in)) {
          pull(in)
        }

      override def onTimer(timerKey: Any): Unit = {
        sendBulkUpdateRequest(failedMessages)
        failedMessages = Nil
      }

      private def handleFailure(args: (Seq[T], Throwable)): Unit = {
        val (messages, exception) = args
        if (retryCount >= settings.maxRetry) {
          failStage(exception)
        } else {
          retryCount = retryCount + 1
          failedMessages = messages
          scheduleOnce(NotUsed, settings.retryInterval.millis)
        }
      }

      private def handleSuccess(): Unit =
        completeStage()

      private def handleResponse(args: (Seq[T], Response)): Unit = {
        val (messages, response) = args
        val responseJson = EntityUtils.toString(response.getEntity).parseJson

        // If some commands in bulk request failed, pass failed messages to follows.
        val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
        val messageResults = items.elements.zip(messages).map {
          case (item, message) =>
            item.asJsObject.fields("index").asJsObject.fields.get("error") match {
              case Some(errorMessage) => (message, false)
              case None => (message, true)
            }
        }

        val successMsgs = messageResults.filter(_._2).map(_._1)
        val failedMsgs = messageResults.filter(!_._2).map(_._1)

        if (failedMsgs.nonEmpty && settings.retryPartialFailure && retryCount < settings.maxRetry) {
          // Retry partial failed messages
          retryCount = retryCount + 1
          failedMessages = failedMsgs
          scheduleOnce(NotUsed, settings.retryInterval.millis)

          if (successMsgs.nonEmpty) {
            // push the messages that DID succeed
            emit(out, Future.successful(StreamableIncomingMessagesResult[T](successMsgs, Seq())))
          }

        } else {
          retryCount = 0
          // Push failed
          emit(out, Future.successful(StreamableIncomingMessagesResult[T](successMsgs, failedMsgs)))

          // Fetch next messages from queue and send them
          val nextMessages = (1 to settings.bufferSize).flatMap { _ =>
            queue.dequeueFirst(_ => true)
          }

          if (nextMessages.isEmpty) {
            state match {
              case Finished => handleSuccess()
              case _ => state = Idle
            }
          } else {
            sendBulkUpdateRequest(nextMessages)
          }
        }
      }

      private def sendBulkUpdateRequest(messages: Seq[T]): Unit = {
        val json = messages
          .map { message =>
            JsObject(
              "index" -> JsObject(
                Seq(
                  Option("_index" -> JsString(indexName)),
                  Option("_type" -> JsString(typeName)),
                  message.id.map { id =>
                    "_id" -> JsString(id)
                  }
                ).flatten: _*
              )
            ).toString + "\n" + message.json
          }
          .mkString("", "\n", "\n")

        client.performRequestAsync(
          "POST",
          "/_bulk",
          Map[String, String]().asJava,
          new StringEntity(json, StandardCharsets.UTF_8),
          new ResponseListener() {
            override def onFailure(exception: Exception): Unit =
              failureHandler.invoke((messages, exception))
            override def onSuccess(response: Response): Unit =
              responseHandler.invoke((messages, response))
          },
          new BasicHeader("Content-Type", "application/x-ndjson")
        )
      }

      setHandlers(in, out, this)

      override def onPull(): Unit = tryPull()

      override def onPush(): Unit = {
        val message = grab(in)
        queue.enqueue(message)

        state match {
          case Idle => {
            state = Sending
            val messages = (1 to settings.bufferSize).flatMap { _ =>
              queue.dequeueFirst(_ => true)
            }
            sendBulkUpdateRequest(messages)
          }
          case _ => ()
        }

        tryPull()
      }

      override def onUpstreamFailure(exception: Throwable): Unit =
        failStage(exception)

      override def onUpstreamFinish(): Unit =
        state match {
          case Idle => handleSuccess()
          case Sending => state = Finished
          case Finished => ()
        }
    }

}

object ElasticsearchStreamableFlowStage {

  private sealed trait State
  private case object Idle extends State
  private case object Sending extends State
  private case object Finished extends State

}
