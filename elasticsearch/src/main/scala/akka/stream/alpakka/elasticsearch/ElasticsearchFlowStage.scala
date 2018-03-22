/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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

object IncomingMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: Option[String], source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(id, source, NotUsed)

  // Apply method to use when not using passThrough
  def apply[T](id: Option[String], source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingMessage(id, source, NotUsed, Option(version))

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Option(id), source)

  // Java-api - without passThrough
  def create[T](id: String, source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingMessage(Option(id), source, version)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(None, source)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(Option(id), source, passThrough)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long): IncomingMessage[T, C] =
    IncomingMessage(Option(id), source, passThrough, Option(version))

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long, indexName: String): IncomingMessage[T, C] =
    IncomingMessage(Option(id), source, passThrough, Option(version), Option(indexName))

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(None, source, passThrough)
}

final case class IncomingMessage[T, C](id: Option[String],
                                       source: T,
                                       passThrough: C,
                                       version: Option[Long] = None,
                                       indexName: Option[String] = None) {

  def withVersion(version: Long): IncomingMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String) =
    this.copy(indexName = Option(indexName))
}

object IncomingMessageResult {
  // Apply method to use when not using passThrough
  def apply[T](source: T, success: Boolean, errorMsg: Option[String]): IncomingMessageResult[T, NotUsed] =
    IncomingMessageResult(source, NotUsed, success, errorMsg)
}

final case class IncomingMessageResult[T, C](source: T, passThrough: C, success: Boolean, errorMsg: Option[String])

trait MessageWriter[T] {
  def convert(message: T): String
}

class ElasticsearchFlowStage[T, C](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchSinkSettings,
    writer: MessageWriter[T]
) extends GraphStage[FlowShape[IncomingMessage[T, C], Future[Seq[IncomingMessageResult[T, C]]]]] {

  private val in = Inlet[IncomingMessage[T, C]]("messages")
  private val out = Outlet[Future[Seq[IncomingMessageResult[T, C]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      private var state: State = Idle
      private val queue = new mutable.Queue[IncomingMessage[T, C]]()
      private val failureHandler = getAsyncCallback[(Seq[IncomingMessage[T, C]], Throwable)](handleFailure)
      private val responseHandler = getAsyncCallback[(Seq[IncomingMessage[T, C]], Response)](handleResponse)
      private var failedMessages: Seq[IncomingMessage[T, C]] = Nil
      private var retryCount: Int = 0
      private val insertKeyword: String = if (!settings.docAsUpsert) "index" else "update"

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

      private def handleFailure(args: (Seq[IncomingMessage[T, C]], Throwable)): Unit = {
        val (messages, exception) = args
        if (retryCount >= settings.maxRetry) {
          log.warning(s"Received error from elastic. Giving up after $retryCount tries. Error: ${exception.toString}")
          failStage(exception)
        } else {
          retryCount = retryCount + 1
          log.warning(
            s"Received error from elastic. (re)tryCount: $retryCount maxTries: ${settings.maxRetry}. Error: ${exception.toString}"
          )
          failedMessages = messages
          scheduleOnce(NotUsed, settings.retryInterval.millis)
        }
      }

      private def handleSuccess(): Unit =
        completeStage()

      private case class MessageWithResult[T2, C2](m: IncomingMessage[T2, C2], r: IncomingMessageResult[T2, C2])

      private def handleResponse(args: (Seq[IncomingMessage[T, C]], Response)): Unit = {
        val (messages, response) = args
        val responseJson = EntityUtils.toString(response.getEntity).parseJson

        // If some commands in bulk request failed, pass failed messages to follows.
        val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
        val messageResults: Seq[MessageWithResult[T, C]] = items.elements.zip(messages).map {
          case (item, message) =>
            val res = item.asJsObject.fields(insertKeyword).asJsObject
            val error: Option[String] = res.fields.get("error").map(_.toString())
            MessageWithResult(
              message,
              IncomingMessageResult(message.source, message.passThrough, error.isEmpty, error)
            )
        }

        val failedMsgs = messageResults.filterNot(_.r.success)

        if (failedMsgs.nonEmpty && settings.retryPartialFailure && retryCount < settings.maxRetry) {
          retryPartialFailedMessages(messageResults, failedMsgs)
        } else {
          forwardAllResults(messageResults)
        }
      }

      private def retryPartialFailedMessages(
          messageResults: Seq[MessageWithResult[T, C]],
          failedMsgs: Seq[MessageWithResult[T, C]]
      ): Unit = {
        // Retry partial failed messages
        // NOTE: When we partially return message like this, message will arrive out of order downstream
        // and it can break commit-logic when using Kafka
        retryCount = retryCount + 1
        failedMessages = failedMsgs.map(_.m) // These are the messages we're going to retry
        scheduleOnce(NotUsed, settings.retryInterval.millis)

        val successMsgs = messageResults.filter(_.r.success)
        if (successMsgs.nonEmpty) {
          // push the messages that DID succeed
          val resultForSucceededMsgs = successMsgs.map(_.r)
          emit(out, Future.successful(resultForSucceededMsgs))
        }
      }

      private def forwardAllResults(messageResults: Seq[MessageWithResult[T, C]]): Unit = {
        retryCount = 0 // Clear retryCount

        // Push result
        val listOfResults = messageResults.map(_.r)
        emit(out, Future.successful(listOfResults))

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

      private def sendBulkUpdateRequest(messages: Seq[IncomingMessage[T, C]]): Unit = {
        val json = messages
          .map { message =>
            val indexNameToUse: String = message.indexName.getOrElse(indexName)

            JsObject(
              insertKeyword -> JsObject(
                Seq(
                  Option("_index" -> JsString(indexNameToUse)),
                  Option("_type" -> JsString(typeName)),
                  message.version.map { version =>
                    "_version" -> JsNumber(version)
                  },
                  settings.versionType.map { versionType =>
                    "version_type" -> JsString(versionType)
                  },
                  message.id.map { id =>
                    "_id" -> JsString(id)
                  }
                ).flatten: _*
              )
            ).toString + "\n" + messageToJsonString(message)
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

      private def messageToJsonString(message: IncomingMessage[T, C]): String =
        if (!settings.docAsUpsert) {
          writer.convert(message.source)
        } else {
          JsObject(
            "doc" -> writer.convert(message.source).parseJson,
            "doc_as_upsert" -> JsTrue
          ).toString
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
