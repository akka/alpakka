/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.NotUsed
import akka.stream.alpakka.ibm.eventstore.scaladsl.{EventStoreFlow => ScalaEventStoreFlow}
import akka.stream.alpakka.ibm.eventstore.scaladsl.{EventStoreSink => ScalaEventStoreSink}
import akka.stream.javadsl
import com.ibm.event.oltp.InsertResult
import org.apache.spark.sql.Row

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

object EventStoreSink {

  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, 1)(ec).mapMaterializedValue(_.toJava).asJava

  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext,
      parallelism: Int = 1
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, parallelism)(ec).mapMaterializedValue(_.toJava).asJava
}

object EventStoreFlow {

  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, 1)(ec).asJava

  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext,
      parallelism: Int = 1
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, parallelism)(ec).asJava
}
