/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.util
import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

final case class ElasticsearchSinkSettings(bufferSize: Int = 10)

final case class IncomingMessage[T](id: Option[String], source: T)

final class ElasticsearchSinkStage[T](indexName: String,
                                      typeName: String,
                                      client: RestClient,
                                      settings: ElasticsearchSinkSettings)(implicit writer: JsonWriter[T])
    extends GraphStageWithMaterializedValue[SinkShape[IncomingMessage[T]], Future[Done]] {

  val in = Inlet[IncomingMessage[T]]("ElasticsearchSink.in")

  override def shape: SinkShape[IncomingMessage[T]] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic =
      new ElasticsearchSinkLogic(indexName, typeName, client, settings, shape, in, promise)

    (logic, promise.future)
  }

}

sealed class ElasticsearchSinkLogic[T](indexName: String,
                                       typeName: String,
                                       client: RestClient,
                                       settings: ElasticsearchSinkSettings,
                                       shape: SinkShape[IncomingMessage[T]],
                                       in: Inlet[IncomingMessage[T]],
                                       promise: Promise[Done])(implicit writer: JsonWriter[T])
    extends GraphStageLogic(shape)
    with ResponseListener {

  private val state = new AtomicReference[State](Idle)
  private val buffer = new util.concurrent.ConcurrentLinkedQueue[IncomingMessage[T]]()
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)
  private val responseHandler = getAsyncCallback[Response](handleResponse)

  override def preStart(): Unit =
    pull(in)

  private def tryPull(): Unit =
    if (buffer.size < settings.bufferSize && !isClosed(in) && !hasBeenPulled(in)) {
      pull(in)
    }

  private def handleFailure(exception: Throwable): Unit = {
    failStage(exception)
    promise.tryFailure(exception)
  }

  private def handleSuccess(): Unit = {
    completeStage()
    promise.trySuccess(Done)
  }

  private def handleResponse(response: Response): Unit =
    tryPull()

  override def onFailure(exception: Exception): Unit = failureHandler.invoke(exception)

  override def onSuccess(response: Response): Unit = {
    val messages = (1 to settings.bufferSize).flatMap { _ =>
      Option(buffer.poll())
    }

    if (messages.isEmpty) {
      state.get match {
        case Finished => handleSuccess()
        case _ => state.set(Idle)
      }
    } else {
      sendBulkUpdateRequest(messages)
    }

    responseHandler.invoke(response)
  }

  private def sendBulkUpdateRequest(messages: Seq[IncomingMessage[T]]): Unit =
    try {
      val json = messages
        .map { message =>
          s"""{"index": {"_index": "${indexName}", "_type": "${typeName}"${message.id
               .map { id =>
                 s""", "_id": "${id}""""
               }
               .getOrElse("")}}
             |${message.source.toJson.asJsObject.toString}""".stripMargin
        }
        .mkString("", "\n", "\n")

      client.performRequestAsync(
        "POST",
        "_bulk",
        Map[String, String]().asJava,
        new StringEntity(json),
        this
      )
    } catch {
      case ex: Exception => failStage(ex)
    }

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val message = grab(in)
        buffer.add(message)

        state.get match {
          case Idle => {
            state.set(Sending)
            val messages = (1 to settings.bufferSize).flatMap { _ =>
              Option(buffer.poll())
            }
            sendBulkUpdateRequest(messages)
          }
          case _ => ()
        }

        tryPull()
      }

      override def onUpstreamFailure(exception: Throwable): Unit =
        handleFailure(exception)

      override def onUpstreamFinish(): Unit =
        state.get match {
          case Idle => handleSuccess()
          case Sending => state.set(Finished)
          case Finished => ()
        }
    }
  )

}

private sealed trait State
private case object Idle extends State
private case object Sending extends State
private case object Finished extends State
