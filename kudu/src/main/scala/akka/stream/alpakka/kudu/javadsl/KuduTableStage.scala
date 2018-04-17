/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.javadsl

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.alpakka.kudu.internal.KuduFlowStage
import org.apache.kudu.client._
import org.apache.kudu.Schema
import org.apache.kudu.client.PartialRow
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object KuduTableStage {

  def table[T](kuduClient: KuduClient,
               tableName: String,
               schema: Schema,
               createTableOptions: CreateTableOptions,
               converter: java.util.function.Function[T, PartialRow]): KuduTableSettings[T] = {
    import scala.compat.java8.FunctionConverters._
    import scala.collection.JavaConverters._
    KuduTableSettings(kuduClient, tableName, schema, createTableOptions, asScalaFromFunction(converter))
  }

  def sink[A](settings: KuduTableSettings[A]): akka.stream.javadsl.Sink[A, Future[Done]] =
    Flow[A].via(flow(settings)).toMat(Sink.ignore)(Keep.right).asJava

  def flow[A](settings: KuduTableSettings[A]): akka.stream.javadsl.Flow[A, A, NotUsed] =
    Flow.fromGraph(new KuduFlowStage[A](settings)).asJava

}
