/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import java.nio.charset.StandardCharsets

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Response, ResponseListener, RestClient}

import scala.collection.immutable

/**
 * INTERNAL API.
 *
 * Updates Elasticsearch without any built-in retry logic.
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSimpleFlowStage[T, C](
    _indexName: String,
    _typeName: String,
    client: RestClient,
    settings: ElasticsearchWriteSettings,
    writer: MessageWriter[T]
) extends GraphStage[
      FlowShape[(immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]]), immutable.Seq[WriteResult[T, C]]]
    ] {

  private val in =
    Inlet[(immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]])]("messagesAndResultPassthrough")
  private val out = Outlet[immutable.Seq[WriteResult[T, C]]]("result")
  override val shape = FlowShape(in, out)

  private val restApi: RestBulkApi[T, C] = settings.apiVersion match {
    case ApiVersion.V5 =>
      require(_indexName != null, "You must define an index name")
      require(_typeName != null, "You must define a type name")
      new RestBulkApiV5[T, C](_indexName, _typeName, settings.versionType, writer)
    case ApiVersion.V7 =>
      require(_indexName != null, "You must define an index name")
      new RestBulkApiV7[T, C](_indexName, settings.versionType, writer)
    case other => throw new IllegalArgumentException(s"API version $other is not supported")
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new StageLogic()

  private class StageLogic extends GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

    private var inflight = false

    private val failureHandler =
      getAsyncCallback[(immutable.Seq[WriteResult[T, C]], Throwable)](handleFailure)
    private val responseHandler =
      getAsyncCallback[(immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], Response)](handleResponse)

    setHandlers(in, out, this)

    override def onPull(): Unit = tryPull()

    override def onPush(): Unit = {
      val (messages, resultsPassthrough) = grab(in)
      inflight = true
      val json: String = restApi.toJson(messages)

      log.debug("Posting data to Elasticsearch: {}", json)

      // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
      client.performRequestAsync(
        "POST",
        "/_bulk",
        java.util.Collections.emptyMap[String, String](),
        new StringEntity(json, StandardCharsets.UTF_8),
        new ResponseListener() {
          override def onFailure(exception: Exception): Unit =
            failureHandler.invoke((resultsPassthrough, exception))

          override def onSuccess(response: Response): Unit =
            responseHandler.invoke((messages, resultsPassthrough, response))
        },
        new BasicHeader("Content-Type", "application/x-ndjson")
      )
    }

    private def handleFailure(
        args: (immutable.Seq[WriteResult[T, C]], Throwable)
    ): Unit = {
      inflight = false
      val (resultsPassthrough, exception) = args

      log.error(s"Received error from elastic after having already processed {} documents. Error: {}",
                resultsPassthrough.size,
                exception)
      failStage(exception)
    }

    private def handleResponse(
        args: (immutable.Seq[WriteMessage[T, C]], immutable.Seq[WriteResult[T, C]], Response)
    ): Unit = {
      inflight = false
      val (messages, resultsPassthrough, response) = args
      val jsonString = EntityUtils.toString(response.getEntity)
      if (log.isDebugEnabled) {
        import spray.json._
        log.debug("response {}", jsonString.parseJson.prettyPrint)
      }
      val messageResults = restApi.toWriteResults(messages, jsonString)

      if (log.isErrorEnabled) {
        messageResults.filterNot(_.success).map { failure =>
          failure.getError.map { errorJson =>
            log.error(s"Received error from elastic when attempting to index documents. Error: {}", errorJson)
          }
        }
      }

      emit(out, messageResults ++ resultsPassthrough)
      if (isClosed(in)) completeStage()
      else tryPull()
    }

    private def tryPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onUpstreamFinish(): Unit =
      if (!inflight) completeStage()
  }
}
