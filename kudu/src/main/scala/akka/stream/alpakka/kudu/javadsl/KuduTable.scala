/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.alpakka.kudu.impl.KuduFlowStage
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kudu.client.KuduClient

object KuduTable {

  def sink[A](settings: KuduTableSettings[A], kuduClient: KuduClient): Sink[A, CompletionStage[Done]] =
    flow(settings, kuduClient).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def flow[A](settings: KuduTableSettings[A], kuduClient: KuduClient): Flow[A, A, NotUsed] =
    Flow.fromGraph(new KuduFlowStage[A](settings, kuduClient))

}
