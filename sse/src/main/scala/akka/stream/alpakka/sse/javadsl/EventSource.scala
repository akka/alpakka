/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sse
package javadsl

import akka.NotUsed
import akka.http.javadsl.model.{HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.model.{HttpResponse => SHttpResponse}
import akka.stream.Materializer
import akka.stream.javadsl.Source
import akka.http.javadsl.model.sse.ServerSentEvent
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{Function => JFunction}

import akka.actor.ClassicActorSystemProvider

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters

/**
 * This stream processing stage establishes a continuous source of server-sent events from the given URI.
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
  import FutureConverters._
  import OptionConverters._

  /**
   * @param uri URI with absolute path, e.g. "http://myserver/events
   * @param send function to send a HTTP request
   * @param lastEventId initial value for Last-Evend-ID header, optional
   * @param system actor system (classic or new API)
   * @return continuous source of server-sent events
   */
  def create(uri: Uri,
             send: JFunction[HttpRequest, CompletionStage[HttpResponse]],
             lastEventId: Optional[String],
             system: ClassicActorSystemProvider): Source[ServerSentEvent, NotUsed] = {
    val eventSource =
      scaladsl
        .EventSource(
          uri.asScala,
          send(_).toScala.map(_.asInstanceOf[SHttpResponse])(system.classicSystem.dispatcher),
          lastEventId.asScala
        )(system)
        .map(v => v: ServerSentEvent)
    eventSource.asJava
  }

  /**
   * @param uri URI with absolute path, e.g. "http://myserver/events
   * @param send function to send a HTTP request
   * @param lastEventId initial value for Last-Evend-ID header, optional
   * @param mat `Materializer`
   * @return continuous source of server-sent events
   * @deprecated pass in the actor system instead of the materializer, since 3.0.0
   */
  @deprecated("pass in the actor system instead of the materializer", "3.0.0")
  def create(uri: Uri,
             send: JFunction[HttpRequest, CompletionStage[HttpResponse]],
             lastEventId: Optional[String],
             mat: Materializer): Source[ServerSentEvent, NotUsed] = {
    val eventSource =
      scaladsl
        .EventSource(
          uri.asScala,
          send(_).toScala.map(_.asInstanceOf[SHttpResponse])(mat.executionContext),
          lastEventId.asScala
        )(mat.system)
        .map(v => v: ServerSentEvent)
    eventSource.asJava
  }
}
