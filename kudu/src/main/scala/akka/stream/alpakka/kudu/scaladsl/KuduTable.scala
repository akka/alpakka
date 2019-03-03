/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.scaladsl

import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.kudu.{KuduAttributes, KuduClientExt, KuduTableSettings}
import akka.stream.alpakka.kudu.impl.{KuduFlowStage, Setup}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

/**
 * Scala API
 */
object KuduTable {

  /**
   * Create a Sink writing elements to a Kudu table.
   */
  def sink[A](settings: KuduTableSettings[A]): Sink[A, Future[Done]] =
    flow(settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a Flow writing elements to a Kudu table.
   */
  def flow[A](settings: KuduTableSettings[A]): Flow[A, A, NotUsed] =
    Setup
      .flow { implicit mat => implicit attr =>
        Flow.fromGraph(new KuduFlowStage[A](settings, client()))
      }
      .mapMaterializedValue(_ => NotUsed)

  private def client()(implicit mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[KuduAttributes.Client]
      .map(_.client)
      .getOrElse(KuduClientExt(mat.system).client)
}
