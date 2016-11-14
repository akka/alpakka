/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.scaladsl

import akka.Done
import akka.stream.scaladsl.Sink
import com.datastax.driver.core.{ BoundStatement, PreparedStatement, Session }

import scala.concurrent.{ ExecutionContext, Future }
import akka.stream.alpakka.cassandra.GuavaFutureOptsImplicits._

object CassandraSink {
  def apply[T](parallelism: Int, statement: PreparedStatement, statementBinder: (T, PreparedStatement) => BoundStatement)(implicit session: Session, ex: ExecutionContext): Sink[T, Future[Done]] =
    Sink.foreachParallel[T](parallelism) { t =>
      session.executeAsync(statementBinder(t, statement)).toFuture()
    }
}
