/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra

import akka.stream._
import akka.stream.stage.{ AsyncCallback, GraphStage, GraphStageLogic, OutHandler }
import com.datastax.driver.core.{ ResultSet, Row, Session, Statement }
import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

class CassandraSourceStage(futStmt: Future[Statement], session: Session) extends GraphStage[SourceShape[Row]] {
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

