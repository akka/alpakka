/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.util

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.RestClient
import spray.json._

import scala.collection.JavaConverters._

final case class ElasticsearchSinkSettings(bufferSize: Int = 10)

final case class IncomingMessage(id: String, source: JsObject)

final class ElasticsearchSinkStage(indexName: String,
                                   typeName: String,
                                   client: RestClient,
                                   settings: ElasticsearchSinkSettings)
    extends GraphStage[SinkShape[IncomingMessage]] {

  val in = Inlet[IncomingMessage]("ElasticsearchSink.in")

  override def shape: SinkShape[IncomingMessage] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val buffer = new util.ArrayDeque[IncomingMessage]()

      override def preStart(): Unit =
        pull(in)

      setHandler(in,
        new InHandler {
        override def onPush(): Unit = {
          val message = grab(in)
          buffer.addLast(message)
          if (buffer.size >= settings.bufferSize) {
            sendBulkRequest(buffer.toArray(new Array[IncomingMessage](buffer.size)))
            buffer.clear()
          }
          pull(in)
        }

        override def onUpstreamFailure(exception: Throwable): Unit =
          failStage(exception)

        override def onUpstreamFinish(): Unit = {
          if (!buffer.isEmpty) {
            sendBulkRequest(buffer.toArray(new Array[IncomingMessage](buffer.size)))
            buffer.clear()
          }
          completeStage()
        }

        private def sendBulkRequest(messages: Seq[IncomingMessage]): Unit =
          try {
            val json = messages.map { message =>
              s"""{"index": {"_index": "${indexName}", "_type": "${typeName}", "_id": "${message.id}"}}
                 |${message.source.toString}""".stripMargin
            }.mkString("", "\n", "\n")

            client.performRequest(
              "POST",
              "_bulk",
              Map[String, String]().asJava,
              new StringEntity(json)
            )
          } catch {
            case ex: Exception => failStage(ex)
          }
      })
    }

}
