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

final case class IncomingMessage[T](id: Option[String], source: T)

trait MessageWriter[T] {
  def convert(message: T): String
}

class ElasticsearchFlowStage[T, R](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchSinkSettings,
    pusher: Seq[IncomingMessage[T]] => R,
    writer: MessageWriter[T]
) extends GraphStage[FlowShape[IncomingMessage[T], Future[R]]] {

  private val in = Inlet[IncomingMessage[T]]("messages")
  private val out = Outlet[Future[R]]("failed")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private var state: State = Idle
      private val queue = new mutable.Queue[IncomingMessage[T]]()
      private val failureHandler = getAsyncCallback[(Seq[IncomingMessage[T]], Throwable)](handleFailure)
      private val responseHandler = getAsyncCallback[(Seq[IncomingMessage[T]], Response)](handleResponse)
      private var failedMessages: Seq[IncomingMessage[T]] = Nil
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

      private def handleFailure(args: (Seq[IncomingMessage[T]], Throwable)): Unit = {
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

      private def handleResponse(args: (Seq[IncomingMessage[T]], Response)): Unit = {
        val (messages, response) = args
        val responseJson = EntityUtils.toString(response.getEntity).parseJson

        // If some commands in bulk request failed, pass failed messages to follows.
        val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
        val failed = items.elements.zip(messages).flatMap {
          case (item, message) =>
            item.asJsObject.fields("index").asJsObject.fields.get("error").map { _ =>
              message
            }
        }

        if (failed.nonEmpty && settings.retryPartialFailure && retryCount < settings.maxRetry) {
          // Retry partial failed messages
          retryCount = retryCount + 1
          failedMessages = failed
          scheduleOnce(NotUsed, settings.retryInterval.millis)

        } else {
          retryCount = 0
          push(out, Future.successful(pusher(failed)))

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

      private def sendBulkUpdateRequest(messages: Seq[IncomingMessage[T]]): Unit = {
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
