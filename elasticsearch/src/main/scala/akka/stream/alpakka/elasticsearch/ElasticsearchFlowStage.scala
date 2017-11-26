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
import ElasticsearchFlowStage._
import akka.NotUsed
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSinkSettings
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils

trait IncomingMessageTrait[T] {
  def id: Option[String]
  def source: T
}

final case class IncomingMessage[T](id: Option[String], source: T) extends IncomingMessageTrait[T]

final case class IncomingMessageWithCargo[T, C](id: Option[String], source: T, cargo: C)
    extends IncomingMessageTrait[T]

final case class MessageResult[T, X <: IncomingMessageTrait[T]](message: X, success: Boolean)

// Using these special result-case-classes to reduce the generic-complexity when working the flows
final case class IncomingMessageResult[T](source: T, success: Boolean)
final case class IncomingMessageWithCargoResult[T, C](source: T, cargo: C, success: Boolean)

trait MessageWriter[T] {
  def convert(message: T): String
}

// This code must return the MessageResult-type, to be able to work
// with and without cargo.
// MessageResult is transformed into IncomingMessageResult or IncomingMessageWithCargoResult
// in the javadsl- and scaladsl-ElasticsearchFlow-implementations
class ElasticsearchFlowStage[T, X <: IncomingMessageTrait[T]](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchSinkSettings,
    writer: MessageWriter[T]
) extends GraphStage[FlowShape[X, Future[Seq[MessageResult[T, X]]]]] {

  private val in = Inlet[X]("messages")
  private val out = Outlet[Future[Seq[MessageResult[T, X]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private var state: State = Idle
      private val queue = new mutable.Queue[X]()
      private val failureHandler = getAsyncCallback[(Seq[X], Throwable)](handleFailure)
      private val responseHandler = getAsyncCallback[(Seq[X], Response)](handleResponse)
      private var failedMessages: Seq[X] = Nil
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

      private def handleFailure(args: (Seq[X], Throwable)): Unit = {
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

      private def handleResponse(args: (Seq[X], Response)): Unit = {
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
          failedMessages = failedMsgs // These are the messages we're going to retry
          scheduleOnce(NotUsed, settings.retryInterval.millis)

          if (successMsgs.nonEmpty) {
            // push the messages that DID succeed
            val resultForSucceededMsgs = successMsgs.map { x =>
              MessageResult[T, X](x, success = true)
            }
            emit(out, Future.successful(resultForSucceededMsgs))
          }

        } else {
          retryCount = 0

          // Build result of success-msgs and failed-msgs
          val result: Seq[MessageResult[T, X]] = Seq(
            successMsgs.map { x =>
              MessageResult[T, X](x, success = true)
            },
            failedMsgs.map { x =>
              MessageResult[T, X](x, success = false)
            }
          ).flatten

          // Push failed
          emit(out, Future.successful(result))

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

      private def sendBulkUpdateRequest(messages: Seq[X]): Unit = {
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
            ).toString + "\n" + writer.convert(message.source)
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

object ElasticsearchFlowStage {

  private sealed trait State
  private case object Idle extends State
  private case object Sending extends State
  private case object Finished extends State

}
