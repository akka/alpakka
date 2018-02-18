/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}
import akka.stream.alpakka.cassandra.GuavaFutures._

import scala.concurrent.ExecutionContext

object CassandraFlow {
  def createWithPassThrough[T](
      parallelism: Int,
      statement: PreparedStatement,
      statementBinder: (T, PreparedStatement) => BoundStatement
  )(implicit session: Session, ec: ExecutionContext): Flow[T, T, NotUsed] =
    Flow[T].mapAsync(parallelism)(
      t ⇒ session.executeAsync(statementBinder(t, statement)).asScala().map(_ => t)
    )
}
