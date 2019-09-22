/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
import scala.util.{Failure, Success, Try}

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
) extends GraphStage[FlowShape[immutable.Seq[WriteMessage[T, C]], Try[immutable.Seq[WriteResult[T, C]]]]] {
  require(_indexName != null, "You must define an index name")
  require(_typeName != null, "You must define a type name")

  private val in = Inlet[immutable.Seq[WriteMessage[T, C]]]("messages")
  private val out = Outlet[Try[immutable.Seq[WriteResult[T, C]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new StageLogic()

  private class StageLogic
      extends GraphStageLogic(shape)
      with ElasticsearchJsonBase[T, C]
      with InHandler
      with OutHandler
      with StageLogging {

    private var inflight = false

    private val failureHandler = getAsyncCallback[Throwable](handleFailure)
    private val responseHandler = getAsyncCallback[(immutable.Seq[WriteMessage[T, C]], Response)](handleResponse)

    // ElasticsearchJsonBase parameters
    override val indexName: String = _indexName
    override val typeName: String = _typeName
    override val versionType: Option[String] = settings.versionType
    override val messageWriter: MessageWriter[T] = writer

    setHandlers(in, out, this)

    override def onPull(): Unit = tryPull()

    override def onPush(): Unit = {
      val messages = grab(in)
      inflight = true
      val json: String = updateJson(messages)

      log.debug("Posting data to Elasticsearch: {}", json)

      client.performRequestAsync(
        "POST",
        "/_bulk",
        java.util.Collections.emptyMap[String, String](),
        new StringEntity(json, StandardCharsets.UTF_8),
        new ResponseListener() {
          override def onFailure(exception: Exception): Unit = failureHandler.invoke(exception)
          override def onSuccess(response: Response): Unit = responseHandler.invoke((messages, response))
        },
        new BasicHeader("Content-Type", "application/x-ndjson")
      )
    }

    private def handleFailure(exception: Throwable): Unit = {
      inflight = false
      push(out, Failure(exception))
      if (isClosed(in)) completeStage()
      else tryPull()
    }

    private def handleResponse(args: (immutable.Seq[WriteMessage[T, C]], Response)): Unit = {
      inflight = false
      val (messages, response) = args
      val jsonString = EntityUtils.toString(response.getEntity)
      val messageResults = toWriteResults(messages, jsonString)
      push(out, Success(messageResults))
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
