/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.kudu.KuduTableSettings
import akka.stream.alpakka.kudu.internal.KuduFlowStage
import org.apache.kudu.client._
import org.apache.kudu.Schema
import org.apache.kudu.client.PartialRow
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

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

  def sink[A](settings: KuduTableSettings[A]): Sink[A, CompletionStage[Done]] =
    flow(settings).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def flow[A](settings: KuduTableSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new KuduFlowStage[A](settings))

}
