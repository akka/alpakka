/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hbase.scaladsl

import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.impl.{HBaseFlowStage, HBaseSourceStage}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.hadoop.hbase.client.{Result, Scan}

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

  /**
   * Reads an element from HBase.
   */
  def source[A](scan: Scan, settings: HTableSettings[A]): Source[Result, NotUsed] =
    Source.fromGraph(new HBaseSourceStage[A](scan, settings))

}
