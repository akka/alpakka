/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.stream.alpakka.kudu.scaladsl
import akka.{Done, NotUsed}

/**
 * Java API
 */
object KuduTable {

  /**
   * Create a Sink writing elements to a Kudu table.
   */
  def sink[A](settings: KuduTableSettings[A]): Sink[A, CompletionStage[Done]] =
    flow(settings).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a Flow writing elements to a Kudu table.
   */
  def flow[A](settings: KuduTableSettings[A]): Flow[A, A, NotUsed] =
    scaladsl.KuduTable.flow(settings).asJava

}
