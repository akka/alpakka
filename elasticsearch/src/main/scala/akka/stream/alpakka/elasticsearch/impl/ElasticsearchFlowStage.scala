/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl.ElasticsearchFlowStage._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Response, ResponseListener, RestClient}

import scala.collection.immutable

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchFlowStage[T, C](
    _indexName: String,
    _typeName: String,
    client: RestClient,
    settings: ElasticsearchWriteSettings,
    writer: MessageWriter[T]
) extends GraphStage[FlowShape[immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]]]] {

  private val in = Inlet[immutable.Seq[WriteMessage[T, C]]]("messages")
  private val out = Outlet[immutable.Seq[WriteResult[T, C]]]("result")
  override val shape = FlowShape(in, out)

  private val restApi: RestBulkApi[T, C] = settings.apiVersion match {
    case ApiVersion.V5 =>
      require(_indexName != null, "You must define an index name")
      require(_typeName != null, "You must define a type name")
      new RestBulkApiV5[T, C](_indexName, _typeName, settings.versionType, writer)
    case other => throw new IllegalArgumentException(s"API version $other is not supported")
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new StageLogic()

  private class StageLogic extends TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

    private var upstreamFinished = false
    private var inflight = 0

    private val failureHandler =
      getAsyncCallback[(immutable.Seq[WriteMessageWithRetry[T, C]], Throwable)](handleFailure)
    private val responseHandler =
      getAsyncCallback[(immutable.Seq[WriteMessageWithRetry[T, C]], Response)](handleResponse)

    private def tryPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onTimer(timerKey: Any): Unit = timerKey match {
      case RetrySend(failedMessages: immutable.Seq[WriteMessageWithRetry[T, C]] @unchecked) =>
        if (log.isDebugEnabled) log.debug("retrying inflight={} {}", inflight, failedMessages)
        sendBulkUpdateRequest(failedMessages)

      case _ =>
    }

    private def handleFailure(args: (immutable.Seq[WriteMessageWithRetry[T, C]], Throwable)): Unit = {
      val (messages, exception) = args

      messages.groupBy(_.retryCount).foreach {
        case (retryCount, messagesByRetryCount) =>
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
            scheduleOnce(RetrySend(messagesByRetryCount), settings.retryLogic.nextRetry(retryCount))
          }
      }
    }

    private def handleResponse(args: (immutable.Seq[WriteMessageWithRetry[T, C]], Response)): Unit = {
      val (messages, response) = args
      val jsonString = EntityUtils.toString(response.getEntity)
      if (log.isDebugEnabled) {
        import spray.json._
        log.debug("response {}", jsonString.parseJson.prettyPrint)
      }
      val messageResults = restApi.toWriteResults(messages.map(_.writeMessage), jsonString)
      val partialFailure = messageResults.exists(_.error.nonEmpty)
      if (partialFailure) {
        val messageResultsWithRetry = messageResults.zip(messages.map(_.retryCount))
        retryPartialFailedMessages(messageResultsWithRetry)
      } else {
        emitResults(messageResults)
      }
    }

    private def retryPartialFailedMessages(messageResultsWithRetry: immutable.Seq[(WriteResult[T, C], Int)]): Unit = {
      val (failedMsgsIt, successMsgsIt) = messageResultsWithRetry.iterator.partition(_._1.error.nonEmpty)
      val failedMsgs = failedMsgsIt.toList
      if (log.isDebugEnabled) log.debug("retryPartialFailedMessages inflight={} {}", inflight, failedMsgs)
      // Retry partial failed messages
      // NOTE: When we partially return message like this, message will arrive out of order downstream
      // and it can break commit-logic when using Kafka

      emitResults(successMsgsIt.map(_._1).toList)
      failedMsgs.groupBy(_._2).foreach {
        case (retryCount, failedMsgsByRetry) =>
          if (settings.retryLogic.shouldRetry(retryCount, Nil)) {
            val updatedFailedMsgsByRetry = failedMsgsByRetry.map {
              case (wr, _) =>
                WriteMessageWithRetry(wr.message, retryCount + 1)
            }
            scheduleOnce(RetrySend(updatedFailedMsgsByRetry), settings.retryLogic.nextRetry(retryCount))
          } else {
            emitResults(failedMsgsByRetry.map(_._1))
          }
      }
    }

    private def emitResults(successMsgs: immutable.Seq[WriteResult[T, C]]): Unit = {
      emit(out, successMsgs)
      tryPull()
      inflight -= successMsgs.size
      if (upstreamFinished && inflight == 0) completeStage()
    }

    private def sendBulkUpdateRequest(messages: immutable.Seq[WriteMessageWithRetry[T, C]]): Unit = {
      val json: String = restApi.toJson(messages.map(_.writeMessage))

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

    setHandlers(in, out, this)

    override def onPull(): Unit = tryPull()

    override def onPush(): Unit = {
      val messages = grab(in)
      val messagesWithRetry = messages.map(WriteMessageWithRetry(_, 0))
      inflight += messagesWithRetry.size
      sendBulkUpdateRequest(messagesWithRetry)
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
  private case class WriteMessageWithRetry[T, C](writeMessage: WriteMessage[T, C], retryCount: Int)
  private case class RetrySend[T, C](messages: Seq[WriteMessageWithRetry[T, C]])
}
