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

  /**
   * Creates a sink for insertion of records into EventStore.
   * @param databaseName Name of the database, database has to exist before the call to this function.
   * @param tableName    Name of the table, database has to exist before the call to this function.
   * @param ec           Execution context.
   * @return             CompletionStage[Done]
   */
  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, 1)(ec).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink for insertion of records into EventStore.
   * @param databaseName Name of the database, database has to exist before the call to this function.
   * @param tableName    Name of the table, database has to exist before the call to this function.
   * @param ec           Execution context.
   * @param parallelism  Number of concurrent insert operations performed.
   * @return             CompletionStage[Done]
   */
  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext,
      parallelism: Int = 1
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, parallelism)(ec).mapMaterializedValue(_.toJava).asJava
}

object EventStoreFlow {

  /**
   * Creates a flow for insertion of records into EventStore and inspection of the result.
   * @param databaseName Name of the database, database has to exist before the call to this function
   * @param tableName    Name of the table, database has to exist before the call to this function
   * @param ec           Execution context.
   * @return             Returns the InsertResult from EventStore, it can be used to determine if the operation
   *                     succeeded or failed.
   */
  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, 1)(ec).asJava

  /**
   * Creates a flow for insertion of records into EventStore and inspection of the result.
   * @param databaseName Name of the database, database has to exist before the call to this function
   * @param tableName    Name of the table, database has to exist before the call to this function
   * @param ec           Execution context.
   * @param parallelism  Number of concurrent insert operations performed.
   * @return             Returns the InsertResult from EventStore, it can be used to determine if the operation
   *                     succeeded or failed.
   */
  def create(
      databaseName: String,
      tableName: String,
      ec: ExecutionContext,
      parallelism: Int = 1
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, parallelism)(ec).asJava
}
