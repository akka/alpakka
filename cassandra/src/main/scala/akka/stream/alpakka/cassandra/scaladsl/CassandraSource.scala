/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.cassandra.impl.CassandraSourceStage
import akka.stream.scaladsl.Source
import com.datastax.driver.core._

import scala.concurrent.Future

@ApiMayChange // https://github.com/akka/alpakka/issues/1213
object CassandraSource {

  /**
   * Scala API: creates a [[CassandraSourceStage]] from a given statement.
   */
  def apply(stmt: Statement)(implicit session: Session): Source[Row, NotUsed] =
    Source.fromGraph(new CassandraSourceStage(Future.successful(stmt), session))

  /**
   * Scala API: creates a [[CassandraSourceStage]] from the result of a given Future.
   */
  def fromFuture(futStmt: Future[Statement])(implicit session: Session): Source[Row, NotUsed] =
    Source.fromGraph(new CassandraSourceStage(futStmt, session))

}
