/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import java.io.ByteArrayOutputStream

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}
import spray.json._
import DefaultJsonProtocol._
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSourceSettings
import org.apache.http.message.BasicHeader

import scala.collection.JavaConverters._

final case class OutgoingMessage[T](id: String, source: T)

case class ScrollResponse[T](error: Option[String], result: Option[ScrollResult[T]])
case class ScrollResult[T](scrollId: String, messages: Seq[OutgoingMessage[T]])

trait MessageReader[T] {
  def convert(json: String): ScrollResponse[T]
}

final class ElasticsearchSourceStage[T](indexName: String,
                                        typeName: String,
                                        query: String,
                                        client: RestClient,
                                        settings: ElasticsearchSourceSettings,
                                        reader: MessageReader[T])
    extends GraphStage[SourceShape[OutgoingMessage[T]]] {

  val out: Outlet[OutgoingMessage[T]] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[OutgoingMessage[T]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ElasticsearchSourceLogic[T](indexName, typeName, query, client, settings, out, shape, reader)

}

sealed class ElasticsearchSourceLogic[T](indexName: String,
                                         typeName: String,
                                         query: String,
                                         client: RestClient,
                                         settings: ElasticsearchSourceSettings,
                                         out: Outlet[OutgoingMessage[T]],
                                         shape: SourceShape[OutgoingMessage[T]],
                                         reader: MessageReader[T])
    extends GraphStageLogic(shape)
    with ResponseListener
    with OutHandler {

  private var scrollId: String = null
  private val responseHandler = getAsyncCallback[Response](handleResponse)
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)

  def sendScrollScanRequest(): Unit =
    try {
      if (scrollId == null) {
        client.performRequestAsync(
          "POST",
          s"/$indexName/$typeName/_search",
          Map("scroll" -> "5m", "sort" -> "_doc").asJava,
          new StringEntity(s"""{"size": ${settings.bufferSize}, "query": ${query}}"""),
          this,
          new BasicHeader("Content-Type", "application/json")
        )
      } else {
        client.performRequestAsync(
          "POST",
          s"/_search/scroll",
          Map[String, String]().asJava,
          new StringEntity(Map("scroll" -> "5m", "scroll_id" -> scrollId).toJson.toString),
          this,
          new BasicHeader("Content-Type", "application/json")
        )
      }
    } catch {
      case ex: Exception => handleFailure(ex)
    }

  override def onFailure(exception: Exception) = failureHandler.invoke(exception)
  override def onSuccess(response: Response) = responseHandler.invoke(response)

  def handleFailure(ex: Throwable): Unit =
    failStage(ex)

  def handleResponse(res: Response): Unit = {
    val json = {
      val out = new ByteArrayOutputStream()
      try {
        res.getEntity.writeTo(out)
        new String(out.toByteArray, "UTF-8")
      } finally {
        out.close()
      }
    }

    reader.convert(json) match {
      case ScrollResponse(Some(error), _) =>
        failStage(new IllegalStateException(error))
      case ScrollResponse(None, Some(result)) if result.messages.isEmpty =>
        completeStage()
      case ScrollResponse(_, Some(result)) =>
        scrollId = result.scrollId
        emitMultiple(out, result.messages.toIterator)
    }
  }

  setHandler(out, this)

  override def onPull(): Unit = sendScrollScanRequest()

}
