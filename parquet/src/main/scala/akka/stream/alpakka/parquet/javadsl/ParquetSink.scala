/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.{Function => JFunction}

import akka.Done
import akka.stream.ActorMaterializer
import akka.stream.alpakka.parquet.Parquet.ParquetSettings
import akka.stream.alpakka.parquet.scaladsl.{ParquetSink => ScalaParquetSink}
import akka.stream.javadsl.Sink

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters
import scala.reflect.ClassTag

object ParquetSink {
  def create[T](settings: ParquetSettings,
                fileName: String,
                recordClass: Class[T],
                mat: ActorMaterializer): Sink[T, CompletionStage[Done]] =
    ScalaParquetSink[T](settings, fileName)(mat, ClassTag(recordClass))
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

  def partitionSink[T, K](settings: ParquetSettings,
                          filePrefix: String,
                          recordClass: Class[T],
                          mat: ActorMaterializer)(
      groupBy: JFunction[T, K],
      partitionName: JFunction[K, String]
  ): Sink[T, CompletionStage[Done]] =
    ScalaParquetSink
      .partitionSink(settings, filePrefix)(groupBy.asScala, partitionName.asScala)(mat, ClassTag(recordClass))
      .mapMaterializedValue(FutureConverters.toJava)
      .asJava

}
