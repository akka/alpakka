/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.util

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import org.apache.commons.io.IOUtils
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}

import scala.collection.JavaConverters._

final case class ElasticsearchSourceSettings(maxBufferSize: Int, maxBatchSize: Int) {
  require(maxBatchSize <= maxBufferSize)
  require(maxBatchSize >= 1 && maxBatchSize <= 10)
}

final class ElasticsearchSourceStage(indexName: String,
                                     typeName: String,
                                     query: String,
                                     client: RestClient,
                                     settings: ElasticsearchSourceSettings)
    extends GraphStage[SourceShape[Map[String, Any]]] {

  val out: Outlet[Map[String, Any]] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[Map[String, Any]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var scrollId: String = null
      private val buffer = new util.ArrayDeque[Map[String, Any]]()

      def receiveMessages(): Unit =
        try {
          val res = if (scrollId == null) {
            client.performRequest(
              "POST",
              s"$indexName/$typeName/_search",
              Map("scroll" -> "5m", "sort" -> "_doc").asJava,
              new StringEntity(query)
            )
          } else {
            client.performRequest(
              "POST",
              s"/_search/scroll",
              Map[String, String]().asJava,
              new StringEntity(JsonUtils.serialize(Map("scroll" -> "5m", "scroll_id" -> scrollId)))
            )
          }
          handleSuccess(res)

        } catch {
          case ex: Exception => handleFailure(ex)
        }

      def handleFailure(ex: Exception): Unit =
        failStage(ex)

      def handleSuccess(res: Response): Unit = {
        val in = res.getEntity.getContent

        val json = try {
          IOUtils.toString(in, "UTF-8")
        } finally {
          in.close()
        }

        val map = JsonUtils.deserialize[Map[String, Any]](json)

        map.get("error") match {
          case Some(error) => {
            failStage(new IllegalStateException(error.toString))
          }
          case None => {
            val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]
            if (list.isEmpty && scrollId != null) {
              completeStage()
            } else {
              scrollId = map("_scroll_id").toString
              list.reverse.foreach(buffer.addFirst)
            }
          }
        }
      }

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit = {
          if (!buffer.isEmpty) {
            if (buffer.size == settings.maxBufferSize - settings.maxBatchSize) {
              receiveMessages()
            }
          } else {
            receiveMessages()
          }
          if (!buffer.isEmpty) {
            push(out, buffer.removeLast())
          }
        }
      })
    }

}
