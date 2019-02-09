/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hbase.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.internal.HBaseFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.compat.java8.FutureConverters._

object HTableStage {

  def sink[A](config: HTableSettings[A]): akka.stream.javadsl.Sink[A, CompletionStage[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right).mapMaterializedValue(toJava).asJava

  def flow[A](settings: HTableSettings[A]): akka.stream.javadsl.Flow[A, A, NotUsed] =
    Flow.fromGraph(new HBaseFlowStage[A](settings)).asJava

}
