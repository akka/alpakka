/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.util

import akka.Done
import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.RestClient
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

final case class ElasticsearchSinkSettings(bufferSize: Int = 10)

final case class IncomingMessage(id: String, source: JsObject)

final class ElasticsearchSinkStage(indexName: String,
                                   typeName: String,
                                   client: RestClient,
                                   settings: ElasticsearchSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[IncomingMessage], Future[Done]] {

  val in = Inlet[IncomingMessage]("ElasticsearchSink.in")

  override def shape: SinkShape[IncomingMessage] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()

    val logic = new GraphStageLogic(shape) {

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

        override def onUpstreamFailure(exception: Throwable): Unit = {
          failStage(exception)
          promise.tryFailure(exception)
        }

        override def onUpstreamFinish(): Unit = {
          if (!buffer.isEmpty) {
            sendBulkRequest(buffer.toArray(new Array[IncomingMessage](buffer.size)))
            buffer.clear()
          }
          completeStage()
          promise.trySuccess(Done)
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

    (logic, promise.future)
  }

}
