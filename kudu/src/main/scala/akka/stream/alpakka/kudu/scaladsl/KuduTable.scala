/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.scaladsl

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.alpakka.kudu.impl.KuduFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.apache.kudu.client.KuduClient

import scala.concurrent.Future

object KuduTable {

  def sink[A](settings: KuduTableSettings[A])(implicit kuduClient: KuduClient): Sink[A, Future[Done]] =
    flow(settings)(kuduClient).toMat(Sink.ignore)(Keep.right)

  def flow[A](settings: KuduTableSettings[A])(implicit kuduClient: KuduClient): Flow[A, A, NotUsed] =
    Flow.fromGraph(new KuduFlowStage[A](settings, kuduClient))

}
