/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import java.io.ByteArrayOutputStream

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.{ElasticsearchSourceSettings, ReadResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.{Response, ResponseListener, RestClient}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResponse[T](error: Option[String], result: Option[ScrollResult[T]])

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResult[T](scrollId: String, messages: Seq[ReadResult[T]])

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] trait MessageReader[T] {
  def convert(json: String): ScrollResponse[T]
}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSourceStage[T](indexName: String,
                                                               typeName: Option[String],
                                                               searchParams: Map[String, String],
                                                               client: RestClient,
                                                               settings: ElasticsearchSourceSettings,
                                                               reader: MessageReader[T])
    extends GraphStage[SourceShape[ReadResult[T]]] {
  require(indexName != null, "You must define an index name")

  val out: Outlet[ReadResult[T]] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[ReadResult[T]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ElasticsearchSourceLogic[T](indexName, typeName, searchParams, client, settings, out, shape, reader)

}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSourceLogic[T](indexName: String,
                                                               typeName: Option[String],
                                                               searchParams: Map[String, String],
                                                               client: RestClient,
                                                               settings: ElasticsearchSourceSettings,
                                                               out: Outlet[ReadResult[T]],
                                                               shape: SourceShape[ReadResult[T]],
                                                               reader: MessageReader[T])
    extends GraphStageLogic(shape)
    with ResponseListener
    with OutHandler
    with StageLogging {

  private var scrollId: String = null
  private val responseHandler = getAsyncCallback[Response](handleResponse)
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)

  private var waitingForElasticData = false
  private var pullIsWaitingForData = false
  private var dataReady: Option[ScrollResponse[T]] = None

  def sendScrollScanRequest(): Unit =
    try {
      waitingForElasticData = true

      if (scrollId == null) {
        log.debug("Doing initial search")

        // Add extra params to search
        val extraParams = Seq(
          if (!searchParams.contains("size")) {
            Some(("size" -> settings.bufferSize.toString))
          } else {
            None
          },
          // Tell elastic to return the documents '_version'-property with the search-results
          // http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html
          // https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
          if (!searchParams.contains("version") && settings.includeDocumentVersion) {
            Some(("version" -> "true"))
          } else {
            None
          }
        )

        val completeParams = searchParams ++ extraParams.flatten

        val searchBody = "{" + completeParams
          .map {
            case (name, json) =>
              "\"" + name + "\":" + json
          }
          .mkString(",") + "}"

        val endpoint: String = (indexName, typeName) match {
          case (i, Some(t)) => s"/$i/$t/_search"
          case (i, None) => s"/$i/_search"
        }
        client.performRequestAsync(
          "POST",
          endpoint,
          Map("scroll" -> settings.scroll, "sort" -> "_doc").asJava,
          new StringEntity(searchBody),
          this,
          new BasicHeader("Content-Type", "application/json")
        )
      } else {
        log.debug("Fetching next scroll")

        client.performRequestAsync(
          "POST",
          s"/_search/scroll",
          Map[String, String]().asJava,
          new StringEntity(Map("scroll" -> settings.scroll, "scroll_id" -> scrollId).toJson.toString),
          this,
          new BasicHeader("Content-Type", "application/json")
        )
      }
    } catch {
      case ex: Exception => handleFailure(ex)
    }

  override def onFailure(exception: Exception) = failureHandler.invoke(exception)
  override def onSuccess(response: Response) = responseHandler.invoke(response)

  def handleFailure(ex: Throwable): Unit = {
    waitingForElasticData = false
    failStage(ex)
  }

  def handleResponse(res: Response): Unit = {
    waitingForElasticData = false
    val json = {
      val out = new ByteArrayOutputStream()
      try {
        res.getEntity.writeTo(out)
        new String(out.toByteArray, "UTF-8")
      } finally {
        out.close()
      }
    }

    val scrollResponse = reader.convert(json)

    if (pullIsWaitingForData) {
      log.debug("Received data from elastic. Downstream has already called pull and is waiting for data")
      pullIsWaitingForData = false
      if (handleScrollResponse(scrollResponse)) {
        // we should go and get more data
        sendScrollScanRequest()
      }
    } else {
      log.debug("Received data from elastic. Downstream have not yet asked for it")
      // This is a prefetch of data which we received before downstream has asked for it
      dataReady = Some(scrollResponse)
    }

  }

  // Returns true if we should continue to work
  def handleScrollResponse(scrollResponse: ScrollResponse[T]): Boolean =
    scrollResponse match {
      case ScrollResponse(Some(error), _) =>
        failStage(new IllegalStateException(error))
        false
      case ScrollResponse(None, Some(result)) if result.messages.isEmpty =>
        completeStage()
        false
      case ScrollResponse(_, Some(result)) =>
        scrollId = result.scrollId
        log.debug("Pushing data downstream")
        emitMultiple(out, result.messages.toIterator)
        true
    }

  setHandler(out, this)

  override def onPull(): Unit =
    dataReady match {
      case Some(data) =>
        // We already have data ready
        log.debug("Downstream is pulling data and we already have data ready")
        if (handleScrollResponse(data)) {
          // We should go and get more data

          dataReady = None

          if (!waitingForElasticData) {
            sendScrollScanRequest()
          }

        }
      case None =>
        if (pullIsWaitingForData) throw new Exception("This should not happen: Downstream is pulling more than once")
        pullIsWaitingForData = true

        if (!waitingForElasticData) {
          log.debug("Downstream is pulling data. We must go and get it")
          sendScrollScanRequest()
        } else {
          log.debug("Downstream is pulling data. Already waiting for data")
        }
    }

}
