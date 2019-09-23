/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.Operation._
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl.ElasticsearchFlowStage._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Response, ResponseListener, RestClient}
import spray.json._

import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchFlowStage[T, C, P](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchWriteSettings,
    sourceWriter: MessageWriter[T],
    paramsWriter: MessageWriter[P]
) extends GraphStage[FlowShape[immutable.Seq[WriteMessage[T, C, P]], immutable.Seq[WriteResult[T, C, P]]]] {
  require(indexName != null, "You must define an index name")
  require(typeName != null, "You must define a type name")

  private val in = Inlet[immutable.Seq[WriteMessage[T, C, P]]]("messages")
  private val out = Outlet[immutable.Seq[WriteResult[T, C, P]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new StageLogic()

  private class StageLogic extends TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

    private val typeNameTuple = "_type" -> JsString(typeName)
    private val versionTypeTuple: Option[(String, JsString)] = settings.versionType.map { versionType =>
      "version_type" -> JsString(versionType)
    }


    private var upstreamFinished = false
    private var inflight = 0

    private val failureHandler = getAsyncCallback[(immutable.Seq[WriteMessage[T, C, P]], Throwable)](handleFailure)
    private val responseHandler = getAsyncCallback[(immutable.Seq[WriteMessage[T, C, P]], Response)](handleResponse)
    private var failedMessages: immutable.Seq[WriteMessage[T, C, P]] = Nil
    private var retryCount: Int = 0

    private def tryPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onTimer(timerKey: Any): Unit = {
      if (log.isDebugEnabled) log.debug("retrying inflight={} {}", inflight, failedMessages)
      sendBulkUpdateRequest(failedMessages)
      failedMessages = Nil
    }

    private def handleFailure(args: (immutable.Seq[WriteMessage[T, C, P]], Throwable)): Unit = {
      val (messages, exception) = args
      if (!settings.retryLogic.shouldRetry(retryCount, List(exception.toString))) {
        log.error("Received error from elastic. Giving up after {} tries. {}, Error: {}",
                  retryCount,
                  settings.retryLogic,
                  exception)
        failStage(exception)
      } else {
        log.warning("Received error from elastic. Try number {}. {}, Error: {}",
                    retryCount,
                    settings.retryLogic,
                    exception)
        retryCount = retryCount + 1
        failedMessages = messages
        scheduleOnce(RetrySend, settings.retryLogic.nextRetry(retryCount))
      }
    }

    private def handleResponse(args: (immutable.Seq[WriteMessage[T, C, P]], Response)): Unit = {
      val (messages, response) = args
      val responseJson = EntityUtils.toString(response.getEntity).parseJson
      if (log.isDebugEnabled) log.debug("response {}", responseJson.prettyPrint)

      // If some commands in bulk request failed, pass failed messages to follows.
      val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
      val messageResults: immutable.Seq[WriteResult[T, C, P]] = items.elements.zip(messages).map {
        case (item, message) =>
          val command = message.operation.command
          val res = item.asJsObject.fields(command).asJsObject
          val error: Option[String] = res.fields.get("error").map(_.toString())
          new WriteResult(message, error)
      }

      val failedMsgs = messageResults.filterNot(_.error.isEmpty)

      if (failedMsgs.nonEmpty && settings.retryLogic.shouldRetry(retryCount, failedMsgs.map(_.error.get).toList)) {
        retryPartialFailedMessages(messageResults, failedMsgs)
      } else {
        retryCount = 0
        emitResults(messageResults)
      }
    }

    private def retryPartialFailedMessages(
        messageResults: immutable.Seq[WriteResult[T, C, P]],
        failedMsgs: immutable.Seq[WriteResult[T, C, P]]
    ): Unit = {
      if (log.isDebugEnabled) log.debug("retryPartialFailedMessages inflight={} {}", inflight, failedMsgs)
      // Retry partial failed messages
      // NOTE: When we partially return message like this, message will arrive out of order downstream
      // and it can break commit-logic when using Kafka
      retryCount = retryCount + 1
      failedMessages = failedMsgs.map(_.message) // These are the messages we're going to retry
      scheduleOnce(RetrySend, settings.retryLogic.nextRetry(retryCount))

      val successMsgs = messageResults.filter(_.error.isEmpty)
      if (successMsgs.nonEmpty) {
        // push the messages that DID succeed
        emitResults(successMsgs)
      }
    }

    private def emitResults(successMsgs: immutable.Seq[WriteResult[T, C, P]]): Unit = {
      emit(out, successMsgs)
      tryPull()
      inflight -= successMsgs.size
      if (upstreamFinished && inflight == 0) completeStage()
    }

    private def sendBulkUpdateRequest(messages: immutable.Seq[WriteMessage[T, C, P]]): Unit = {
      val json = messages
        .map { message =>
          val sharedFields: Seq[(String, JsString)] = Seq(
              "_index" -> JsString(message.indexName.getOrElse(indexName)),
              typeNameTuple
            ) ++ message.customMetadata.map { case (field, value) => field -> JsString(value) }
          val tuple: (String, JsObject) = message.operation match {
            case Index =>
              val fields = Seq(
                message.version.map { version =>
                  "_version" -> JsNumber(version)
                },
                versionTypeTuple,
                message.id.map { id =>
                  "_id" -> JsString(id)
                }
              ).flatten
              "index" -> JsObject(
                (sharedFields ++ fields): _*
              )
            case Create =>
              val fields = Seq(
                message.id.map { id =>
                  "_id" -> JsString(id)
                }
              ).flatten
              "create" -> JsObject(
                (sharedFields ++ fields): _*
              )
            case Update | Upsert | InlineScript | PreparedScript =>
              val fields = Seq(
                message.version.map { version =>
                  "_version" -> JsNumber(version)
                },
                versionTypeTuple,
                Option("_id" -> JsString(message.id.get))
              ).flatten
              "update" -> JsObject(
                (sharedFields ++ fields): _*
              )
            case Delete =>
              val fields = Seq(
                message.version.map { version =>
                  "_version" -> JsNumber(version)
                },
                versionTypeTuple,
                Option("_id" -> JsString(message.id.get))
              ).flatten
              "delete" -> JsObject(
                (sharedFields ++ fields): _*
              )
          }
          JsObject(tuple).compactPrint + messageToJsonString(message)
        }
        .mkString("", "\n", "\n")

      log.debug("Posting data to Elasticsearch: {}", json)

      client.performRequestAsync(
        "POST",
        "/_bulk",
        java.util.Collections.emptyMap[String, String](),
        new StringEntity(json, StandardCharsets.UTF_8),
        new ResponseListener() {
          override def onFailure(exception: Exception): Unit = failureHandler.invoke((messages, exception))
          override def onSuccess(response: Response): Unit = responseHandler.invoke((messages, response))
        },
        new BasicHeader("Content-Type", "application/x-ndjson")
      )
    }

    private def messageToJsonString(message: WriteMessage[T, C, P]): String =
      message.operation match {
        case Index | Create =>
          "\n" + sourceWriter.convert(message.source.get)
        case Upsert =>
          "\n" + JsObject(
            "doc" -> sourceWriter.convert(message.source.get).parseJson,
            "doc_as_upsert" -> JsTrue
          ).toString
        case Update =>
          "\n" + JsObject(
            "doc" -> sourceWriter.convert(message.source.get).parseJson
          ).toString
        case Delete =>
          ""
        case PreparedScript =>
          if (message.params != NotUsed) {
            return "\n" + JsObject(
              "script" -> JsObject(
                "id" -> JsString(message.scriptRef.get),
                "params" -> paramsWriter.convert(message.params).parseJson,
                "upsert" -> sourceWriter.convert(message.source.get).parseJson,
              )
            ).toString
          }
          "\n" + JsObject(
            "script" -> JsObject(
              "id" -> JsString(message.scriptRef.get),
              "upsert" -> sourceWriter.convert(message.source.get).parseJson,
            )
          ).toString
        case InlineScript =>
          if (message.params != NotUsed) {
            return "\n" + JsObject(
              "script" -> JsObject(
                "source" -> JsString(message.scriptRef.get),
                "lang"    -> JsString(message.lang.get),
                "params" -> paramsWriter.convert(message.params).parseJson,
                "upsert" -> sourceWriter.convert(message.source.get).parseJson,
              )
            ).toString
          }
          "\n" + JsObject(
            "script" -> JsObject(
              "source" -> JsString(message.scriptRef.get),
              "lang"    -> JsString(message.lang.get),
              "params" -> paramsWriter.convert(message.params).parseJson,
              "upsert" -> sourceWriter.convert(message.source.get).parseJson,
            )
          ).toString


      }

    setHandlers(in, out, this)

    override def onPull(): Unit = tryPull()

    override def onPush(): Unit = {
      val messages = grab(in)
      inflight += messages.size
      sendBulkUpdateRequest(messages)
    }

    override def onUpstreamFinish(): Unit =
      if (inflight == 0) completeStage()
      else upstreamFinished = true
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] object ElasticsearchFlowStage {

  private object RetrySend
}
