/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.cassandra

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{ AsyncCallback, OutHandler, GraphStageLogic, GraphStage }
import com.datastax.driver.core.{ ResultSet, Session, Statement, Row }
import com.google.common.util.concurrent.{ ListenableFuture, FutureCallback, Futures }
import scala.concurrent.{ Future, Promise }
import scala.collection.JavaConverters._
import scala.util.{ Try, Failure, Success }

class CassandraSource(futStmt: Future[Statement], session: Session) extends GraphStage[SourceShape[Row]] {
  val out: Outlet[Row] = Outlet("CassandraSource.out")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var maybeRs = Option.empty[ResultSet]
      var futFetchedCallback: AsyncCallback[Try[ResultSet]] = _

      override def preStart(): Unit = {
        implicit val ec = materializer.executionContext

        futFetchedCallback = getAsyncCallback[Try[ResultSet]](tryPushAfterFetch)

        val futRs = futStmt.flatMap(stmt => guavaFutToScalaFut(session.executeAsync(stmt)))
        futRs.onComplete(futFetchedCallback.invoke)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          implicit val ec = materializer.executionContext

          maybeRs match {
            case Some(rs) if rs.getAvailableWithoutFetching > 0 => push(out, rs.one())
            case Some(rs) if rs.isExhausted                     => completeStage()
            case Some(rs) =>
              // fetch next page
              val futRs = guavaFutToScalaFut(rs.fetchMoreResults())
              futRs.onComplete(futFetchedCallback.invoke)
            case None => () // doing nothing, waiting for futRs in preStart() to be completed
          }
        }
      })

      private def tryPushAfterFetch(rsOrFailure: Try[ResultSet]): Unit = rsOrFailure match {
        case Success(rs) =>
          maybeRs = Some(rs)
          if (rs.getAvailableWithoutFetching > 0) {
            if (isAvailable(out)) {
              push(out, rs.one())
            }
          } else {
            completeStage()
          }

        case Failure(failure) => failStage(failure)
      }
    }

  private def guavaFutToScalaFut[A](guavaFut: ListenableFuture[A]): Future[A] = {
    val p = Promise[A]()
    val callback = new FutureCallback[A] {
      override def onSuccess(a: A): Unit = p.success(a)
      override def onFailure(err: Throwable): Unit = p.failure(err)
    }
    Futures.addCallback(guavaFut, callback)
    p.future
  }
}

object CassandraSource {
  /**
   * Scala API:
   */
  def apply(stmt: Statement)(implicit session: Session): Source[Row, NotUsed] =
    Source.fromGraph(new CassandraSource(Future.successful(stmt), session))

  def fromFuture(futStmt: Future[Statement])(implicit session: Session): Source[Row, NotUsed] =
    Source.fromGraph(new CassandraSource(futStmt, session))

  /**
   * Java API:
   */
  def create(stmt: Statement, session: Session): akka.stream.javadsl.Source[Row, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new CassandraSource(Future.successful(stmt), session))

  def createFromFuture(
    futStmt: java.util.concurrent.CompletableFuture[Statement],
    session: Session
  ): akka.stream.javadsl.Source[Row, NotUsed] = {
    import scala.compat.java8.FutureConverters._
    akka.stream.javadsl.Source.fromGraph(new CassandraSource(futStmt.toScala, session))
  }
}
