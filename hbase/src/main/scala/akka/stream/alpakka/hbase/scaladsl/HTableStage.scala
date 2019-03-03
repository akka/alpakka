/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hbase.scaladsl

import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.impl.HBaseFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object HTableStage {

  /**
   * Writes incoming element to HBase.
   * HBase mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def sink[A](config: HTableSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right)

  /**
   * Writes incoming element to HBase.
   * HBase mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def flow[A](settings: HTableSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new HBaseFlowStage[A](settings))

}
