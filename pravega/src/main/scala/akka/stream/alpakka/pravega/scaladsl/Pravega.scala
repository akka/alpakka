/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.scaladsl
import akka.annotation.ApiMayChange
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import akka.stream.alpakka.pravega.impl.{PravegaFlow, PravegaSource}
import akka.stream.alpakka.pravega.{PravegaEvent, PravegaReaderGroupManager, ReaderSettings, WriterSettings}
import io.pravega.client.ClientConfig
import io.pravega.client.stream.ReaderGroup

import scala.concurrent.Future

@ApiMayChange
object Pravega {

  def readerGroupManager(scope: String, clientConfig: ClientConfig) = new PravegaReaderGroupManager(scope, clientConfig)

  /**
   * Messages are read from a Pravega stream.
   *
   * Materialized value is a [[Future]] which completes to [[Done]] as soon as the Pravega reader is open.
   */
  def source[A](readerGroup: ReaderGroup, readerSettings: ReaderSettings[A]): Source[PravegaEvent[A], Future[Done]] =
    Source.fromGraph(new PravegaSource(readerGroup, readerSettings))

  /**
   * Incoming messages are written to Pravega stream and emitted unchanged.
   */
  def flow[A](scope: String, streamName: String, writerSettings: WriterSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new PravegaFlow(scope, streamName, writerSettings))

  /**
   * Incoming messages are written to Pravega.
   */
  def sink[A](scope: String, streamName: String, writerSettings: WriterSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(scope, streamName, writerSettings)).toMat(Sink.ignore)(Keep.right)

}
