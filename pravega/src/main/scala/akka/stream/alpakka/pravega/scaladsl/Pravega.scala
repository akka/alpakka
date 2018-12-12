/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaFlow, PravegaSource}
import akka.stream.alpakka.pravega.{PravegaEvent, ReaderSettings, WriterSettings}

import scala.concurrent.Future

object Pravega {

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a future which completes to Done as soon as Pravega reader is open.
   */
  def source[A](scope: String,
                streamName: String)(implicit readerSettings: ReaderSettings[A]): Source[PravegaEvent[A], Future[Done]] =
    Source.fromGraph(new PravegaSource(scope, streamName, readerSettings))

  /**
   * Incoming messages are written to Pravega stream and emitted unchanged.
   */
  def flow[A](scope: String, streamName: String)(
      implicit writerSettings: WriterSettings[A]
  ): Flow[A, A, NotUsed] =
    Flow.fromGraph(new PravegaFlow(scope, streamName, writerSettings))

  /**
   * Incoming messages are written to Pravega.
   */
  def sink[A](scope: String, streamName: String)(
      implicit writerSettings: WriterSettings[A]
  ): Sink[A, Future[Done]] =
    Flow[A].via(flow(scope, streamName)).toMat(Sink.ignore)(Keep.right)
}
