/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase.scaladsl

import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.internal.HBaseFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object HTableStage {

  def sink[A](config: HTableSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right)

  def flow[A](settings: HTableSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new HBaseFlowStage[A](settings))

}
