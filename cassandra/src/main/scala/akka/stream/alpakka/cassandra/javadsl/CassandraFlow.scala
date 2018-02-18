/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import java.util.function.BiFunction

import akka.NotUsed
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow => ScalaCFlow}
import akka.stream.javadsl.Flow

import scala.concurrent.ExecutionContext

object CassandraFlow {
  def createWithPassThrough[T](parallelism: Int,
                               statement: PreparedStatement,
                               statementBinder: BiFunction[T, PreparedStatement, BoundStatement],
                               session: Session,
                               ec: ExecutionContext): Flow[T, T, NotUsed] =
    ScalaCFlow
      .createWithPassThrough[T](parallelism, statement, (t, p) => statementBinder.apply(t, p))(session, ec)
      .asJava
}
