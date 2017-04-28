/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import java.util
import java.util.concurrent.atomic.AtomicReference

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}
import spray.json.JsonWriter

import scala.collection.JavaConverters._
import spray.json._

import scala.concurrent.Future

final case class ElasticsearchSinkSettings(bufferSize: Int = 10, parallelism: Int = 1)

final case class IncomingMessage[T](id: Option[String], source: T)

class ElasticsearchFlowStage[T](
    indexName: String,
    typeName: String,
    client: RestClient,
    settings: ElasticsearchSinkSettings
)(implicit writer: JsonWriter[T])
    extends GraphStage[FlowShape[IncomingMessage[T], Future[Response]]] {

  private val in = Inlet[IncomingMessage[T]]("messages")
  private val out = Outlet[Future[Response]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with ResponseListener {

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

      private def handleFailure(exception: Throwable): Unit =
        failStage(exception)

      private def handleSuccess(): Unit =
        completeStage()

      private def handleResponse(response: Response): Unit = {
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

        push(out, Future.successful(response))
      }

      override def onFailure(exception: Exception): Unit = failureHandler.invoke(exception)

      override def onSuccess(response: Response): Unit = responseHandler.invoke(response)

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

      setHandler(out, new OutHandler {
        override def onPull(): Unit = tryPull()
      })

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

}

private sealed trait State
private case object Idle extends State
private case object Sending extends State
private case object Finished extends State
