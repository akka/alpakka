/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb.scaladsl

import akka.Done
import akka.stream.alpakka.kairosdb.{KairosDBSinkStage, KairosSinkSettings, NullExecutionContext}
import akka.stream.scaladsl.Sink
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

import scala.concurrent.{ExecutionContext, Future}

object KairosSink {
  def apply(
      settings: KairosSinkSettings = KairosSinkSettings.Defaults
  )(implicit kairosClient: HttpClient, executionContext: ExecutionContext): Sink[MetricBuilder, Future[Done]] =
    Sink.fromGraph(new KairosDBSinkStage(settings, kairosClient))
}
