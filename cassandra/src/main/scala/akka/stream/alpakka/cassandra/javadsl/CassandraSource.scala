/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.javadsl.Source
import com.datastax.oss.driver.api.core.cql.{Row, Statement}

import scala.annotation.varargs

/**
 * Java API.
 */
object CassandraSource {

  /**
   * Prepare, bind and execute a select statement in one go.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   */
  @varargs
  def create(session: CassandraSession, cqlStatement: String, bindValues: AnyRef*): Source[Row, NotUsed] =
    session.select(cqlStatement, bindValues: _*)

  /**
   * Create a [[akka.stream.javadsl.Source Source]] from a given statement.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   */
  def create(session: CassandraSession, stmt: Statement[_]): Source[Row, NotUsed] =
    session.select(stmt)

  /**
   * Create a [[akka.stream.javadsl.Source Source]] from a given statement.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   */
  def fromCompletionStage(session: CassandraSession, stmt: CompletionStage[Statement[_]]): Source[Row, NotUsed] =
    session.select(stmt)

}
