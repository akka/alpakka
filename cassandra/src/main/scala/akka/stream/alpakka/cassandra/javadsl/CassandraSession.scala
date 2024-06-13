/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import java.util.{List => JList}
import java.util.Optional
import java.util.concurrent.{CompletionStage, Executor}
import java.util.function.{Function => JFunction}

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import akka.Done
import akka.NotUsed
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.LoggingAdapter
import akka.stream.alpakka.cassandra.CassandraServerMetaData
import akka.stream.alpakka.cassandra.{scaladsl, CqlSessionProvider}
import akka.stream.javadsl.Source
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.Statement

/**
 * Data Access Object for Cassandra. The statements are expressed in
 * <a href="https://cassandra.apache.org/doc/latest/cql/">Apache Cassandra Query Language</a>
 * (CQL) syntax.
 *
 * See even <a href="https://docs.datastax.com/en/dse/6.7/cql/">CQL for Datastax Enterprise</a>.
 *
 * The `init` hook is called before the underlying session is used by other methods,
 * so it can be used for things like creating the keyspace and tables.
 *
 * All methods are non-blocking.
 */
final class CassandraSession(@InternalApi private[akka] val delegate: scaladsl.CassandraSession) {

  /**
   * Use this constructor if you want to create a stand-alone `CassandraSession`.
   */
  def this(system: ActorSystem,
           sessionProvider: CqlSessionProvider,
           executionContext: ExecutionContext,
           log: LoggingAdapter,
           metricsCategory: String,
           init: JFunction[CqlSession, CompletionStage[Done]],
           onClose: java.lang.Runnable) =
    this(
      new scaladsl.CassandraSession(system,
                                    sessionProvider,
                                    executionContext,
                                    log,
                                    metricsCategory,
                                    session => init.apply(session).toScala,
                                    () => onClose.run())
    )

  /**
   * Use this constructor if you want to create a stand-alone `CassandraSession`.
   */
  def this(system: ClassicActorSystemProvider,
           sessionProvider: CqlSessionProvider,
           executionContext: ExecutionContext,
           log: LoggingAdapter,
           metricsCategory: String,
           init: JFunction[CqlSession, CompletionStage[Done]],
           onClose: java.lang.Runnable) =
    this(system.classicSystem, sessionProvider, executionContext, log, metricsCategory, init, onClose)

  implicit private val ec: ExecutionContext = delegate.ec

  /**
   * Closes the underlying Cassandra session.
   * @param executor as this might be used after actor system termination, the actor systems dispatcher can't be used
   */
  def close(executor: Executor): CompletionStage[Done] = delegate.close(ExecutionContext.fromExecutor(executor)).toJava

  /**
   * Meta data about the Cassandra server, such as its version.
   */
  def serverMetaData: CompletionStage[CassandraServerMetaData] =
    delegate.serverMetaData.toJava

  /**
   * The `Session` of the underlying
   * <a href="https://docs.datastax.com/en/developer/java-driver/">Datastax Java Driver</a>.
   * Can be used in case you need to do something that is not provided by the
   * API exposed by this class. Be careful to not use blocking calls.
   */
  def underlying(): CompletionStage[CqlSession] =
    delegate.underlying().toJava

  /**
   * Execute <a href="https://docs.datastax.com/en/dse/6.7/cql/">CQL commands</a>
   * to manage database resources (create, replace, alter, and drop tables, indexes, user-defined types, etc).
   *
   * The returned `CompletionStage` is completed when the command is done, or if the statement fails.
   */
  def executeDDL(stmt: String): CompletionStage[Done] =
    delegate.executeDDL(stmt).toJava

  /**
   * Create a `PreparedStatement` that can be bound and used in
   * `executeWrite` or `select` multiple times.
   */
  def prepare(stmt: String): CompletionStage[PreparedStatement] =
    delegate.prepare(stmt).toJava

  /**
   * Execute several statements in a batch. First you must `prepare` the
   * statements and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useBatchTOC.html">Batching data insertion and updates</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `BatchStatement`.
   *
   * The returned `CompletionStage` is completed when the batch has been
   * successfully executed, or if it fails.
   */
  def executeWriteBatch(batch: BatchStatement): CompletionStage[Done] =
    delegate.executeWriteBatch(batch).toJava

  /**
   * Execute one statement. First you must `prepare` the
   * statement and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  def executeWrite(stmt: Statement[_]): CompletionStage[Done] =
    delegate.executeWrite(stmt).toJava

  /**
   * Prepare, bind and execute one statement in one go.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used.
   *
   * The returned `CompletionStage` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  @varargs
  def executeWrite(stmt: String, bindValues: AnyRef*): CompletionStage[Done] =
    delegate.executeWrite(stmt, bindValues: _*).toJava

  def executeConditionalWrite(stmt: Statement[_]): CompletionStage[ConditionalWriteResult[Done]] =
    delegate.executeConditionalWrite(stmt)
      .map(ConditionalWriteResultBuilder.fromEither)(ExecutionContexts.parasitic)
      .toJava

  @varargs
  def executeConditionalWrite(stmt: String, bindValues: AnyRef*): CompletionStage[ConditionalWriteResult[Done]] =
    delegate.executeConditionalWrite(stmt, bindValues: _*)
      .map(ConditionalWriteResultBuilder.fromEither)(ExecutionContexts.parasitic)
      .toJava

  /**
   * Execute a select statement. First you must `prepare` the
   * statement and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: Statement[_]): Source[Row, NotUsed] =
    delegate.select(stmt).asJava

  /**
   * Execute a select statement. First you must `prepare` the
   * statement and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: CompletionStage[Statement[_]]): Source[Row, NotUsed] =
    delegate.select(stmt.toScala).asJava

  /**
   * Prepare, bind and execute a select statement in one go.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  @varargs
  def select(stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] =
    delegate.select(stmt, bindValues: _*).asJava

  /**
   * Execute a select statement. First you must `prepare` the statement and
   * bind its parameters. Only use this method when you know that the result
   * is small, e.g. includes a `LIMIT` clause. Otherwise you should use the
   * `select` method that returns a `Source`.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed with the found rows.
   */
  def selectAll(stmt: Statement[_]): CompletionStage[JList[Row]] =
    delegate.selectAll(stmt).map(_.asJava).toJava

  /**
   * Prepare, bind and execute a select statement in one go. Only use this method
   * when you know that the result is small, e.g. includes a `LIMIT` clause.
   * Otherwise you should use the `select` method that returns a `Source`.
   *
   * The configured read consistency level is used.
   *
   * The returned `CompletionStage` is completed with the found rows.
   */
  @varargs
  def selectAll(stmt: String, bindValues: AnyRef*): CompletionStage[JList[Row]] =
    delegate.selectAll(stmt, bindValues: _*).map(_.asJava).toJava

  /**
   * Execute a select statement that returns one row. First you must `prepare` the
   * statement and bind its parameters.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed with the first row,
   * if any.
   */
  def selectOne(stmt: Statement[_]): CompletionStage[Optional[Row]] =
    delegate.selectOne(stmt).map(_.asJava).toJava

  /**
   * Prepare, bind and execute a select statement that returns one row.
   *
   * The configured read consistency level is used.
   *
   * The returned `CompletionStage` is completed with the first row,
   * if any.
   */
  @varargs
  def selectOne(stmt: String, bindValues: AnyRef*): CompletionStage[Optional[Row]] =
    delegate.selectOne(stmt, bindValues: _*).map(_.asJava).toJava

}
