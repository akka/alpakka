/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore.javadsl

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._

import akka.Done
import akka.NotUsed
import akka.stream.javadsl
import akka.stream.alpakka.ibm.eventstore.scaladsl.{EventStoreFlow => ScalaEventStoreFlow}
import akka.stream.alpakka.ibm.eventstore.scaladsl.{EventStoreSink => ScalaEventStoreSink}

import com.ibm.event.oltp.InsertResult
import org.apache.spark.sql.Row

object EventStoreSink {

  /**
   * Creates a sink for insertion of records into EventStore.
   * @param databaseName Name of the database, database has to exist before the call to this function.
   * @param tableName    Name of the table, database has to exist before the call to this function.
   * @return             CompletionStage[Done]
   */
  def create(
      databaseName: String,
      tableName: String
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, 1).mapMaterializedValue(_.toJava).asJava

  /**
   * Creates a sink for insertion of records into EventStore.
   * @param databaseName Name of the database, database has to exist before the call to this function.
   * @param tableName    Name of the table, database has to exist before the call to this function.
   * @param parallelism  Number of concurrent insert operations performed.
   * @return             CompletionStage[Done]
   */
  def create(
      databaseName: String,
      tableName: String,
      parallelism: Int
  ): javadsl.Sink[Row, CompletionStage[Done]] =
    ScalaEventStoreSink(databaseName, tableName, parallelism).mapMaterializedValue(_.toJava).asJava
}

object EventStoreFlow {

  /**
   * Creates a flow for insertion of records into EventStore and inspection of the result.
   * @param databaseName Name of the database, database has to exist before the call to this function
   * @param tableName    Name of the table, database has to exist before the call to this function
   * @return             Returns the InsertResult from EventStore, it can be used to determine if the operation
   *                     succeeded or failed.
   */
  def create(
      databaseName: String,
      tableName: String
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, 1).asJava

  /**
   * Creates a flow for insertion of records into EventStore and inspection of the result.
   * @param databaseName Name of the database, database has to exist before the call to this function
   * @param tableName    Name of the table, database has to exist before the call to this function
   * @param parallelism  Number of concurrent insert operations performed.
   * @return             Returns the InsertResult from EventStore, it can be used to determine if the operation
   *                     succeeded or failed.
   */
  def create(
      databaseName: String,
      tableName: String,
      parallelism: Int
  ): javadsl.Flow[Row, InsertResult, NotUsed] =
    ScalaEventStoreFlow(databaseName, tableName, parallelism).asJava
}
