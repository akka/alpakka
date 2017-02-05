/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sse
package scaladsl

import akka.NotUsed
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Sink, Source, Unzip}
import akka.stream.{Materializer, SourceShape}
import de.heikoseeberger.akkasse.MediaTypes.`text/event-stream`
import de.heikoseeberger.akkasse.headers.`Last-Event-ID`
import de.heikoseeberger.akkasse.{EventStreamUnmarshalling, ServerSentEvent}
import scala.concurrent.{Future, Promise}

/**This stream processing stage establishes a continuous source of server-sent events from the given URI
 * .
 *
 * A single source of server-sent events is obtained from the URI. Once completed, either normally or by failure, a next
 * one is obtained thereby sending a Last-Evend-ID header if available. This continues in an endless cycle.
 *
 * The shape of this processing stage is a source of server-sent events; to take effect it must be connected and run.
 * Progress (including termination) is controlled by the connected flow or sink, e.g. a retry delay can be implemented
 * by streaming the materialized values of the handler via a throttle.
 *
 *{{{
 * + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
 *                                               +---------------------+
 * |                                             |       trigger       | |
 *                                               +----------o----------+
 * |                                                        |            |
 *                                            Option[String]|
 * |                                                        v            |
 *              Option[String]                   +----------o----------+
 * |            +------------------------------->o        merge        | |
 *              |                                +----------o----------+
 * |            |                                           |            |
 *              |                             Option[String]|
 * |            |                                           v            |
 *   +----------o----------+                     +----------o----------+
 * | | currentLastEventId  |                     |    eventSources     | |
 *   +----------o----------+                     +----------o----------+
 * |            ^                                           |            |
 *              |      (EventSource, Future[Option[String]])|
 * |            |                                           v            |
 *              |                                +----------o----------+
 * |            +--------------------------------o        unzip        | |
 *              Future[Option[String]]           +----------o----------+
 * |                                                        |            |
 *                                               EventSource|
 * |                                                        v            |
 *                                               +----------o----------+
 * |                                  +----------o       flatten       | |
 *                     ServerSentEvent|          +---------------------+
 * |                                  v                                  |
 *  - - - - - - - - - - - - - - - - - o - - - - - - - - - - - - - - - - -
 *}}}
 */
object EventSource {

  type EventSource = Source[ServerSentEvent, NotUsed]

  private val noEvents = Future.successful(Source.empty[ServerSentEvent])

  /**
   * @param uri URI with absolute path, e.g. "http://myserver/events
   * @param send function to send a HTTP request
   * @param lastEventId initial value for Last-Evend-ID header, optional
   * @param mat implicit `Materializer`
   * @return continuous source of server-sent events
   */
  def apply(uri: Uri, send: HttpRequest => Future[HttpResponse], lastEventId: Option[String] = None)(
      implicit mat: Materializer): EventSource = {
    import EventStreamUnmarshalling._
    import mat.executionContext

    val eventSources = {
      def getEventSource(lastEventId: Option[String]) = {
        val request = {
          val r = Get(uri).addHeader(Accept(`text/event-stream`))
          lastEventId.foldLeft(r)((r, i) => r.addHeader(`Last-Event-ID`(i)))
        }
        send(request).flatMap(Unmarshal(_).to[EventSource]).fallbackTo(noEvents)
      }
      def enrichWithLastEventId(eventSource: EventSource) = {
        val p = Promise[Option[String]]()
        val enriched =
          eventSource.alsoToMat(Sink.lastOption) {
            case (m, f) =>
              p.completeWith(f.map(_.flatMap(_.id)))
              m
          }
        (enriched, p.future)
      }
      Flow[Option[String]].mapAsync(1)(getEventSource).map(enrichWithLastEventId)
    }

    val currentLastEventId =
      Flow[Future[Option[String]]]
        .mapAsync(1)(identity) // There can only be one request in flight
        .scan(lastEventId)((prev, current) => current.orElse(prev))
        .drop(1)

    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val trigger = builder.add(Source.single(lastEventId))
      val merge = builder.add(Merge[Option[String]](2))
      val unzip = builder.add(Unzip[EventSource, Future[Option[String]]]())
      val flatten = builder.add(Flow[EventSource].flatMapConcat(identity))
      // format: OFF
                                                unzip.out0 ~> flatten
      trigger ~> merge ~>    eventSources    ~> unzip.in
                 merge <~ currentLastEventId <~ unzip.out1
      // format: ON
      SourceShape(flatten.out)
    })
  }
}
