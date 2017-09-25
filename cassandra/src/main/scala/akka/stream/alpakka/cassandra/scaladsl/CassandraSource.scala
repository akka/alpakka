/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.scaladsl

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.stream.alpakka.cassandra.{CassandraRereadableSourceStage, CassandraSourceStage}
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.Select

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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

  /**
   * Scala API: creates a [[akka.stream.alpakka.cassandra.CassandraRereadableSourceStage]] from the result of a given parameters.
   */
  def rereadable[KEY](
      afterId: KEY,
      statement: KEY => Select.Where,
      getId: Row => KEY,
      fetchSize: Int,
      batchSize: Int,
      pollInterval: FiniteDuration
  )(implicit session: Session, system: ActorSystem): Source[Row, NotUsed] =
    Source.fromGraph(
      new CassandraRereadableSourceStage(afterId,
                                         statement,
                                         getId,
                                         fetchSize,
                                         batchSize,
                                         session,
                                         pollInterval,
                                         system)
    )

}
