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
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils

object IncomingIndexMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: Option[String], source: T): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(id, source, NotUsed)

  // Apply method to use when not using passThrough
  def apply[T](id: Option[String], source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(id, source, NotUsed, Option(version))

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(Option(id), source)

  // Java-api - without passThrough
  def create[T](id: String, source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(Option(id), source, version)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(None, source)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingIndexMessage(Option(id), source, passThrough)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long): IncomingMessage[T, C] =
    IncomingIndexMessage(Option(id), source, passThrough, Option(version))

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long, indexName: String): IncomingMessage[T, C] =
    IncomingIndexMessage(Option(id), source, passThrough, Option(version), Option(indexName))

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingIndexMessage(None, source, passThrough)
}

object IncomingUpdateMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed)

  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T, upsert: Boolean): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, upsert)

  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, false, Option(version))

  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T, upsert: Boolean, version: Long): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, upsert, Option(version))

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed)

  // Java-api - without passThrough
  def create[T](id: String, source: T, upsert: Boolean): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, upsert)

  // Java-api - without passThrough
  def create[T](id: String, source: T, version: Long): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, false, Some(version))

  // Java-api - without passThrough
  def create[T](id: String, source: T, upsert: Boolean, version: Long): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source, NotUsed, upsert, Some(version))

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, upsert: Boolean): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough, upsert)

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough, false, Option(version))

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, upsert: Boolean, version: Long): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough, upsert, Option(version))

  // Java-api - with passThrough
  def create[T, C](id: String, source: T, passThrough: C, version: Long, indexName: String): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough, false, Option(version), Option(indexName))

  // Java-api - with passThrough
  def create[T, C](id: String,
                   source: T,
                   passThrough: C,
                   upsert: Boolean,
                   version: Long,
                   indexName: String): IncomingMessage[T, C] =
    IncomingUpdateMessage(id, source, passThrough, upsert, Option(version), Option(indexName))

}

object IncomingDeleteMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String): IncomingDeleteMessage[T, NotUsed] =
    IncomingDeleteMessage(id, NotUsed)

  // Apply method to use when not using passThrough
  def apply[T](id: String, version: Long): IncomingDeleteMessage[T, NotUsed] =
    IncomingDeleteMessage(id, NotUsed, Option(version))

  // Java-api - without passThrough
  def create[T](id: String): IncomingDeleteMessage[T, NotUsed] =
    IncomingDeleteMessage(id)

  // Java-api - without passThrough
  def create[T](id: String, version: Long): IncomingDeleteMessage[T, NotUsed] =
    IncomingDeleteMessage(id, version)

  // Java-api - with passThrough
  def create[T, C](id: String, passThrough: C): IncomingDeleteMessage[T, C] =
    IncomingDeleteMessage(id, passThrough)

  // Java-api - with passThrough
  def create[T, C](id: String, passThrough: C, version: Long): IncomingDeleteMessage[T, C] =
    IncomingDeleteMessage(id, passThrough, Option(version))

  // Java-api - with passThrough
  def create[T, C](id: String, passThrough: C, version: Long, indexName: String): IncomingDeleteMessage[T, C] =
    IncomingDeleteMessage(id, passThrough, Option(version), Option(indexName))
}

sealed trait IncomingMessage[T, C] {
  val passThrough: C
  val version: Option[Long]
  val indexName: Option[String]
  def withVersion(version: Long): IncomingMessage[T, C]
  def withIndexName(indexName: String): IncomingMessage[T, C]
}

final case class IncomingDeleteMessage[T, C](id: String,
                                             passThrough: C,
                                             version: Option[Long] = None,
                                             indexName: Option[String] = None)
    extends IncomingMessage[T, C] {

  def withVersion(version: Long): IncomingDeleteMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String): IncomingDeleteMessage[T, C] =
    this.copy(indexName = Option(indexName))
}

final case class IncomingIndexMessage[T, C](id: Option[String],
                                            source: T,
                                            passThrough: C,
                                            version: Option[Long] = None,
                                            indexName: Option[String] = None)
    extends IncomingMessage[T, C] {

  def withVersion(version: Long): IncomingIndexMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String): IncomingIndexMessage[T, C] =
    this.copy(indexName = Option(indexName))
}

final case class IncomingUpdateMessage[T, C](id: String,
                                             source: T,
                                             passThrough: C,
                                             upsert: Boolean = false,
                                             version: Option[Long] = None,
                                             indexName: Option[String] = None)
    extends IncomingMessage[T, C] {

  def withVersion(version: Long): IncomingUpdateMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String): IncomingUpdateMessage[T, C] =
    this.copy(indexName = Option(indexName))
}

case class IncomingMessageResult[T2, C2](message: IncomingMessage[T2, C2], error: Option[String]) {
  val success = error.isEmpty
}

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

      private def handleResponse(args: (Seq[IncomingMessage[T, C]], Response)): Unit = {
        val (messages, response) = args
        val responseJson = EntityUtils.toString(response.getEntity).parseJson

        // If some commands in bulk request failed, pass failed messages to follows.
        val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
        val messageResults: Seq[IncomingMessageResult[T, C]] = items.elements.zip(messages).map {
          case (item, message) =>
            val command = message match {
              case _: IncomingIndexMessage[_, _] => "index"
              case _: IncomingUpdateMessage[_, _] => "update"
              case _: IncomingDeleteMessage[_, _] => "delete"
            }
            val res = item.asJsObject.fields(command).asJsObject
            val error: Option[String] = res.fields.get("error").map(_.toString())
            IncomingMessageResult(message, error)
        }

        val failedMsgs = messageResults.filterNot(_.error.isEmpty)

        if (failedMsgs.nonEmpty && settings.retryPartialFailure && retryCount < settings.maxRetry) {
          retryPartialFailedMessages(messageResults, failedMsgs)
        } else {
          forwardAllResults(messageResults)
        }
      }

      private def retryPartialFailedMessages(
          messageResults: Seq[IncomingMessageResult[T, C]],
          failedMsgs: Seq[IncomingMessageResult[T, C]]
      ): Unit = {
        // Retry partial failed messages
        // NOTE: When we partially return message like this, message will arrive out of order downstream
        // and it can break commit-logic when using Kafka
        retryCount = retryCount + 1
        failedMessages = failedMsgs.map(_.message) // These are the messages we're going to retry
        scheduleOnce(NotUsed, settings.retryInterval.millis)

        val successMsgs = messageResults.filter(_.error.isEmpty)
        if (successMsgs.nonEmpty) {
          // push the messages that DID succeed
          //val resultForSucceededMsgs = successMsgs.map(_.r)
          emit(out, Future.successful(successMsgs))
        }
      }

      private def forwardAllResults(messageResults: Seq[IncomingMessageResult[T, C]]): Unit = {
        retryCount = 0 // Clear retryCount

        // Push result
        emit(out, Future.successful(messageResults))

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
              message match {
                case x: IncomingIndexMessage[_, _] =>
                  "index" -> JsObject(
                    Seq(
                      Option("_index" -> JsString(indexNameToUse)),
                      Option("_type" -> JsString(typeName)),
                      message.version.map { version =>
                        "_version" -> JsNumber(version)
                      },
                      settings.versionType.map { versionType =>
                        "version_type" -> JsString(versionType)
                      },
                      x.id.map { id =>
                        "_id" -> JsString(id)
                      }
                    ).flatten: _*
                  )
                case x: IncomingUpdateMessage[_, _] =>
                  "update" -> JsObject(
                    Seq(
                      Option("_index" -> JsString(indexNameToUse)),
                      Option("_type" -> JsString(typeName)),
                      message.version.map { version =>
                        "_version" -> JsNumber(version)
                      },
                      settings.versionType.map { versionType =>
                        "version_type" -> JsString(versionType)
                      },
                      Option("_id" -> JsString(x.id))
                    ).flatten: _*
                  )
                case x: IncomingDeleteMessage[_, _] =>
                  "delete" -> JsObject(
                    Seq(
                      Option("_index" -> JsString(indexNameToUse)),
                      Option("_type" -> JsString(typeName)),
                      message.version.map { version =>
                        "_version" -> JsNumber(version)
                      },
                      settings.versionType.map { versionType =>
                        "version_type" -> JsString(versionType)
                      },
                      Option("_id" -> JsString(x.id))
                    ).flatten: _*
                  )
              }
            ).toString + messageToJsonString(message)
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
        message match {
          case x: IncomingIndexMessage[T, C] =>
            "\n" + writer.convert(x.source)
          case x: IncomingUpdateMessage[T, C] if x.upsert =>
            "\n" + JsObject(
              "doc" -> writer.convert(x.source).parseJson,
              "doc_as_upsert" -> JsTrue
            ).toString
          case x: IncomingUpdateMessage[T, C] =>
            "\n" + JsObject(
              "doc" -> writer.convert(x.source).parseJson
            ).toString
          case _: IncomingDeleteMessage[T, C] =>
            ""
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
