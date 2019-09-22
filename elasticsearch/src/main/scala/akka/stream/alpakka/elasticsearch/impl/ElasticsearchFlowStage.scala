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
  require(_indexName != null, "You must define an index name")
  require(_typeName != null, "You must define a type name")

  private val in = Inlet[immutable.Seq[WriteMessage[T, C]]]("messages")
  private val out = Outlet[immutable.Seq[WriteResult[T, C]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new StageLogic()

  private class StageLogic
      extends TimerGraphStageLogic(shape)
      with ElasticsearchJsonBase[T, C]
      with InHandler
      with OutHandler
      with StageLogging {

    private var upstreamFinished = false
    private var inflight = 0

    private val failureHandler = getAsyncCallback[(immutable.Seq[WriteMessage[T, C]], Throwable)](handleFailure)
    private val responseHandler = getAsyncCallback[(immutable.Seq[WriteMessage[T, C]], Response)](handleResponse)
    private var failedMessages: immutable.Seq[WriteMessage[T, C]] = Nil
    private var retryCount: Int = 0

    // ElasticsearchJsonBase parameters
    override val indexName: String = _indexName
    override val typeName: String = _typeName
    override val versionType: Option[String] = settings.versionType
    override val messageWriter: MessageWriter[T] = writer

    private def tryPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onTimer(timerKey: Any): Unit = {
      if (log.isDebugEnabled) log.debug("retrying inflight={} {}", inflight, failedMessages)
      sendBulkUpdateRequest(failedMessages)
      failedMessages = Nil
    }

    private def handleFailure(args: (immutable.Seq[WriteMessage[T, C]], Throwable)): Unit = {
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

    private def handleResponse(args: (immutable.Seq[WriteMessage[T, C]], Response)): Unit = {
      val (messages, response) = args
      val jsonString = EntityUtils.toString(response.getEntity)
      val messageResults = writeResults(messages, jsonString)

      val failedMsgs = messageResults.filterNot(_.error.isEmpty)

      if (failedMsgs.nonEmpty && settings.retryLogic.shouldRetry(retryCount, failedMsgs.map(_.error.get).toList)) {
        retryPartialFailedMessages(messageResults, failedMsgs)
      } else {
        retryCount = 0
        emitResults(messageResults)
      }
    }

    private def retryPartialFailedMessages(
        messageResults: immutable.Seq[WriteResult[T, C]],
        failedMsgs: immutable.Seq[WriteResult[T, C]]
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

    private def emitResults(successMsgs: immutable.Seq[WriteResult[T, C]]): Unit = {
      emit(out, successMsgs)
      tryPull()
      inflight -= successMsgs.size
      if (upstreamFinished && inflight == 0) completeStage()
    }

    private def sendBulkUpdateRequest(messages: immutable.Seq[WriteMessage[T, C]]): Unit = {
      val json: String = updateJson(messages)

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
