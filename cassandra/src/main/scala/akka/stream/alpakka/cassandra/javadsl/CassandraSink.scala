/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import akka.Done
import akka.stream.javadsl.Sink
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSink => ScalaCSink}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

object CassandraSink {

  def create[T](parallelism: Int,
                statement: PreparedStatement,
                statementBinder: BiFunction[T, PreparedStatement, BoundStatement],
                session: Session,
                executionContext: ExecutionContext): Sink[T, CompletionStage[Done]] = {
    val sink =
      ScalaCSink.apply[T](parallelism, statement, (t, p) => statementBinder.apply(t, p))(session, executionContext)

    sink.mapMaterializedValue(_.toJava).asJava
  }

}
