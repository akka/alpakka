/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.scaladsl

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.alpakka.kudu.internal.KuduFlowStage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object KuduTableStage {

  def sink[A](settings: KuduTableSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(settings)).toMat(Sink.ignore)(Keep.right)

  def flow[A](settings: KuduTableSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new KuduFlowStage[A](settings))

}
