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

final case class IncomingMessage[T](id: Option[String], source: T)

final class ElasticsearchSinkStage(indexName: String,
                                   typeName: String,
                                   client: RestClient,
                                   settings: ElasticsearchSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[IncomingMessage[JsObject]], Future[Done]] {

  val in = Inlet[IncomingMessage[JsObject]]("ElasticsearchSink.in")

  override def shape: SinkShape[IncomingMessage[JsObject]] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new ElasticsearchSinkLogic(indexName, typeName, client, settings, shape, in, promise) {
      override protected def convert(value: JsObject): JsObject = value
    }
    (logic, promise.future)
  }

}

final class ElasticsearchSinkStageTyped[T](indexName: String,
                                           typeName: String,
                                           client: RestClient,
                                           settings: ElasticsearchSinkSettings)(implicit writer: JsonWriter[T])
    extends GraphStageWithMaterializedValue[SinkShape[IncomingMessage[T]], Future[Done]] {

  val in = Inlet[IncomingMessage[T]]("ElasticsearchSink.in")

  override def shape: SinkShape[IncomingMessage[T]] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new ElasticsearchSinkLogic(indexName, typeName, client, settings, shape, in, promise) {
      override protected def convert(value: T): JsObject = value.toJson.asJsObject
    }
    (logic, promise.future)
  }

}

sealed abstract class ElasticsearchSinkLogic[T](indexName: String,
                                                typeName: String,
                                                client: RestClient,
                                                settings: ElasticsearchSinkSettings,
                                                shape: SinkShape[IncomingMessage[T]],
                                                in: Inlet[IncomingMessage[T]],
                                                promise: Promise[Done])
    extends GraphStageLogic(shape) {

  private val buffer = new util.ArrayDeque[IncomingMessage[T]]()

  protected def convert(value: T): JsObject

  override def preStart(): Unit =
    pull(in)

  setHandler(in,
    new InHandler {
    override def onPush(): Unit = {
      val message = grab(in)
      buffer.addLast(message)
      if (buffer.size >= settings.bufferSize) {
        sendBulkRequest(buffer.toArray(new Array[IncomingMessage[T]](buffer.size)))
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
        sendBulkRequest(buffer.toArray(new Array[IncomingMessage[T]](buffer.size)))
        buffer.clear()
      }
      completeStage()
      promise.trySuccess(Done)
    }

    private def sendBulkRequest(messages: Seq[IncomingMessage[T]]): Unit =
      try {
        val json = messages.map { message =>
          s"""{"index": {"_index": "${indexName}", "_type": "${typeName}"${message.id.map { id =>
               s""", "_id": "${id}""""
             }.getOrElse("")}}
               |${convert(message.source).toString}""".stripMargin
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
