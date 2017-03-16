/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.io.ByteArrayOutputStream
import java.util

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, RestClient}
import spray.json._
import DefaultJsonProtocol._

import scala.collection.JavaConverters._

final case class ElasticsearchSourceSettings(bufferSize: Int = 10)

final case class OutgoingMessage(id: String, source: JsObject)

final class ElasticsearchSourceStage(indexName: String,
                                     typeName: String,
                                     query: String,
                                     client: RestClient,
                                     settings: ElasticsearchSourceSettings)
    extends GraphStage[SourceShape[OutgoingMessage]] {

  val out: Outlet[OutgoingMessage] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[OutgoingMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var scrollId: String = null
      private val buffer = new util.ArrayDeque[OutgoingMessage]()

      def receiveMessages(): Unit =
        try {
          val res = if (scrollId == null) {
            client.performRequest(
              "POST",
              s"$indexName/$typeName/_search",
              Map("scroll" -> "5m", "sort" -> "_doc").asJava,
              new StringEntity(s"""{"size": ${settings.bufferSize}, "query": ${query}}""")
            )
          } else {
            client.performRequest(
              "POST",
              s"/_search/scroll",
              Map[String, String]().asJava,
              new StringEntity(Map("scroll" -> "5m", "scroll_id" -> scrollId).toJson.toString)
            )
          }
          handleSuccess(res)

        } catch {
          case ex: Exception => handleFailure(ex)
        }

      def handleFailure(ex: Exception): Unit =
        failStage(ex)

      def handleSuccess(res: Response): Unit = {
        val json = {
          val out = new ByteArrayOutputStream()
          try {
            res.getEntity.writeTo(out)
            new String(out.toByteArray, "UTF-8")
          } finally {
            out.close()
          }
        }

        val jsObj = json.parseJson.asJsObject

        jsObj.fields.get("error") match {
          case None => {
            val hits = jsObj.fields("hits").asJsObject.fields("hits").asInstanceOf[JsArray]
            if (hits.elements.isEmpty && scrollId != null) {
              //completeStage()
            } else {
              scrollId = jsObj.fields("_scroll_id").asInstanceOf[JsString].value
              hits.elements.reverse.foreach { element =>
                val doc = element.asJsObject
                val id = doc.fields("_id").asInstanceOf[JsString].value
                val source = doc.fields("_source").asJsObject
                buffer.addFirst(OutgoingMessage(id, source))
              }
            }
          }
          case Some(error) => {
            failStage(new IllegalStateException(error.toString))
          }
        }
      }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit = {
          if (buffer.isEmpty) {
            receiveMessages()
          }
          if (!buffer.isEmpty) {
            push(out, buffer.removeLast())
          } else {
            completeStage()
          }
        }
      })
    }

}
