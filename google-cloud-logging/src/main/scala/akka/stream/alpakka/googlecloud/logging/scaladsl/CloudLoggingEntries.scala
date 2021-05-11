/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.scaladsl

import akka.Done
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.googlecloud.logging.impl.LogEntryQueue
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, WriteEntriesRequest}
import akka.stream.alpakka.googlecloud.logging.{CloudLoggingException, WriteEntriesSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

private[scaladsl] trait CloudLoggingEntries {

  private val endpoint: Uri = "https://logging.googleapis.com/v2/entries:write"

  /**
   * Writes log entries to Logging.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write Cloud Logging Reference]]
   *
   * @param writeEntriesSettings Settings for the sink.
   * @tparam T The data model for the payload of each [[LogEntry]].
   * @return a [[Sink]] for [[LogEntry]] that materializes a [[Future]] containing a [[Done]] upon completion.
   */
  def writeEntries[T](
      writeEntriesSettings: WriteEntriesSettings
  )(implicit m: ToEntityMarshaller[WriteEntriesRequest[T]]): Sink[LogEntry[T], Future[Done]] = {
    import writeEntriesSettings._
    Flow[LogEntry[T]]
      .via(LogEntryQueue[T](queueSize, flushSeverity, flushWithin, overflowStrategy))
      .map(requestTemplate.withEntries[T])
      .toMat(writeEntries[T])(Keep.right)
  }

  /**
   * Writes log entries to Logging.
   * @see [[https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write Cloud Logging Reference]]
   *
   * @tparam T The data model for the `jsonPayload` of each [[LogEntry]].
   * @return a [[Sink]] for [[WriteEntriesRequest]] that materializes a [[Future]] containing a [[Done]] upon completion.
   */
  def writeEntries[T](
      implicit m: ToEntityMarshaller[WriteEntriesRequest[T]]
  ): Sink[WriteEntriesRequest[T], Future[Done]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        Flow[WriteEntriesRequest[T]]
          .mapAsync(1) { request =>
            Marshal(request)
              .to[RequestEntity]
              .map { entity =>
                HttpRequest(POST, endpoint, entity = entity)
              }(ExecutionContexts.parasitic)
          }
          .via(
            GoogleHttp(mat.system).cachedHostConnectionPool[Done](
              endpoint.authority.host.address,
              endpoint.effectivePort
            )
          )
          .toMat(Sink.ignore)(Keep.right)
      }
      .mapMaterializedValue(_.flatten)

  private implicit val writeEntriesResponseUnmarshaller: FromResponseUnmarshaller[Done] =
    Unmarshaller.withMaterializer { implicit mat => implicit ec => response: HttpResponse =>
      if (response.status.isSuccess)
        response.entity.discardBytes().future
      else
        Unmarshal(response).to[String].flatMap { msg =>
          Future.failed(CloudLoggingException(ErrorInfo(response.status.value, msg)))
        }
    }.withDefaultRetry
}
