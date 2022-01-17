/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.elasticsearch.{ApiVersion, ElasticsearchParams, ElasticsearchSourceSettings, ReadResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResponse[T](error: Option[String], result: Option[ScrollResult[T]])

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResult[T](scrollId: Option[String], messages: Seq[ReadResult[T]])

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
private[elasticsearch] final class ElasticsearchSourceStage[T](
    elasticsearchParams: ElasticsearchParams,
    searchParams: Map[String, String],
    settings: ElasticsearchSourceSettings,
    reader: MessageReader[T]
)(implicit http: HttpExt, mat: Materializer, ec: ExecutionContext)
    extends GraphStage[SourceShape[ReadResult[T]]] {

  val out: Outlet[ReadResult[T]] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[ReadResult[T]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ElasticsearchSourceLogic[T](elasticsearchParams, searchParams, settings, out, shape, reader)

}

object ElasticsearchSourceStage {
  def validate(indexName: String): Unit = {
    require(indexName != null, "You must define an index name")
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSourceLogic[T](
    elasticsearchParams: ElasticsearchParams,
    searchParams: Map[String, String],
    settings: ElasticsearchSourceSettings,
    out: Outlet[ReadResult[T]],
    shape: SourceShape[ReadResult[T]],
    reader: MessageReader[T]
)(implicit http: HttpExt, mat: Materializer, ec: ExecutionContext)
    extends GraphStageLogic(shape)
    with OutHandler
    with StageLogging {

  private var scrollId: Option[String] = None
  private val responseHandler = getAsyncCallback[String](handleResponse)
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)

  private var waitingForElasticData = false
  private var pullIsWaitingForData = false
  private var dataReady: Option[ScrollResponse[T]] = None

  def prepareUri(path: Path): Uri = {
    Uri(settings.connection.baseUrl)
      .withPath(path)
  }

  def sendScrollScanRequest(): Unit =
    try {
      waitingForElasticData = true

      scrollId match {
        case None => {
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

          val baseMap = Map("scroll" -> settings.scroll)

          // only force sorting by _doc (meaning order is not known) if not specified in search params
          val sortQueryParam = if (searchParams.contains("sort")) {
            None
          } else {
            Some(("sort", "_doc"))
          }

          val routingQueryParam = searchParams.get("routing").map(r => ("routing", r))

          val queryParams = baseMap ++ routingQueryParam ++ sortQueryParam
          val completeParams = searchParams ++ extraParams.flatten - "routing"

          val searchBody = "{" + completeParams
              .map {
                case (name, json) =>
                  "\"" + name + "\":" + json
              }
              .mkString(",") + "}"

          val endpoint: String = settings.apiVersion match {
            case ApiVersion.V5 => s"/${elasticsearchParams.indexName}/${elasticsearchParams.typeName.get}/_search"
            case ApiVersion.V7 => s"/${elasticsearchParams.indexName}/_search"
          }

          val uri = prepareUri(Path(endpoint))
            .withQuery(Uri.Query(queryParams))

          val request = HttpRequest(HttpMethods.POST)
            .withUri(uri)
            .withEntity(
              HttpEntity(ContentTypes.`application/json`, searchBody)
            )
            .withHeaders(settings.connection.headers)

          ElasticsearchApi
            .executeRequest(
              request,
              settings.connection
            )
            .flatMap {
              case HttpResponse(StatusCodes.OK, _, responseEntity, _) =>
                Unmarshal(responseEntity)
                  .to[String]
                  .map(json => responseHandler.invoke(json))
              case response: HttpResponse =>
                Unmarshal(response.entity).to[String].map { body =>
                  failureHandler
                    .invoke(
                      new RuntimeException(s"Request failed for POST $uri, got ${response.status} with body: $body")
                    )
                }
            }
            .recover {
              case cause: Throwable => failureHandler.invoke(cause)
            }
        }

        case Some(actualScrollId) => {
          log.debug("Fetching next scroll")

          val uri = prepareUri(Path("/_search/scroll"))

          val request = HttpRequest(HttpMethods.POST)
            .withUri(uri)
            .withEntity(
              HttpEntity(ContentTypes.`application/json`,
                         Map("scroll" -> settings.scroll, "scroll_id" -> actualScrollId).toJson.compactPrint)
            )
            .withHeaders(settings.connection.headers)

          ElasticsearchApi
            .executeRequest(
              request,
              settings.connection
            )
            .flatMap {
              case HttpResponse(StatusCodes.OK, _, responseEntity, _) =>
                Unmarshal(responseEntity)
                  .to[String]
                  .map(json => responseHandler.invoke(json))
              case response: HttpResponse =>
                Unmarshal(response.entity)
                  .to[String]
                  .map { body =>
                    failureHandler.invoke(
                      new RuntimeException(s"Request failed for POST $uri, got ${response.status} with body: $body")
                    )
                  }
            }
            .recover {
              case cause: Throwable => failureHandler.invoke(cause)
            }
        }
      }
    } catch {
      case ex: Exception => failureHandler.invoke(ex)
    }

  def handleFailure(ex: Throwable): Unit = {
    waitingForElasticData = false
    failStage(ex)
  }

  def handleResponse(json: String): Unit = {
    waitingForElasticData = false

    val scrollResponse: ScrollResponse[T] = reader.convert(json)

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
        // Do not attempt to clear the scroll in the case of an error.
        failStage(new IllegalStateException(error))
        false
      case ScrollResponse(None, Some(result)) if result.messages.isEmpty =>
        clearScrollAsync()
        false
      case ScrollResponse(_, Some(result)) =>
        scrollId = result.scrollId
        log.debug("Pushing data downstream")
        emitMultiple(out, result.messages.iterator)
        true
      case other: ScrollResponse[T] =>
        failStage(new IllegalArgumentException(s"unexpected response: $other"))
        false
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

  /**
   * When downstream finishes, it is important to attempt to clear the scroll.
   * As such, this handler initiates an async call to clear the scroll, and
   * then explicitly keeps the stage alive. [[clearScrollAsync()]] is responsible
   * for completing the stage.
   */
  override def onDownstreamFinish(cause: Throwable): Unit = {
    clearScrollAsync()
    setKeepGoing(true)
  }

  /**
   * If the [[scrollId]] is defined, attempt to clear the scroll.
   * Complete the stage successfully, whether or not the clear call succeeds.
   * If the clear call fails, the scroll will eventually timeout.
   */
  def clearScrollAsync(): Unit = {
    scrollId match {
      case None =>
        log.debug("Scroll Id is empty. Completing stage eagerly.")
        completeStage()
      case Some(actualScrollId) => {
        // Clear the scroll
        val uri = prepareUri(Path(s"/_search/scroll/$actualScrollId"))

        val request = HttpRequest(HttpMethods.DELETE)
          .withUri(uri)
          .withHeaders(settings.connection.headers)

        ElasticsearchApi
          .executeRequest(request, settings.connection)
          .flatMap {
            case HttpResponse(StatusCodes.OK, _, responseEntity, _) =>
              Unmarshal(responseEntity)
                .to[String]
                .map(json => {
                  clearScrollAsyncHandler.invoke(Success(json))
                })
            case response: HttpResponse =>
              Unmarshal(response.entity).to[String].map { body =>
                clearScrollAsyncHandler
                  .invoke(
                    Failure(
                      new RuntimeException(s"Request failed for POST $uri, got ${response.status} with body: $body")
                    )
                  )
              }
          }
          .recover {
            case cause: Throwable => failureHandler.invoke(cause)
          }
      }
    }
  }

  private val clearScrollAsyncHandler = getAsyncCallback[Try[String]]({ result =>
    {
      // Note: the scroll will expire, so there is no reason to consider a failed
      // clear as a reason to fail the stream.
      log.debug("Result of clearing the scroll: {}", result)
      completeStage()
    }
  })
}
