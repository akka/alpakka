/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.stream.alpakka.cassandra.impl.GuavaFutures._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}

import scala.concurrent.Future

/**
 * Scala API to create Cassandra Sinks.
 */
@ApiMayChange // https://github.com/akka/alpakka/issues/1213
object CassandraSink {
  def apply[T](
      parallelism: Int,
      statement: PreparedStatement,
      statementBinder: (T, PreparedStatement) => BoundStatement
  )(implicit session: Session): Sink[T, Future[Done]] =
    Flow[T]
      .mapAsyncUnordered(parallelism)(t ⇒ session.executeAsync(statementBinder(t, statement)).asScala())
      .toMat(Sink.ignore)(Keep.right)
}
