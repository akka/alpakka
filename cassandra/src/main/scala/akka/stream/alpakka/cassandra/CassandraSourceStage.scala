/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra

import akka.stream._
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import com.datastax.driver.core.{ResultSet, Row, Session, Statement}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import akka.stream.alpakka.cassandra.GuavaFutures._

final class CassandraSourceStage(futStmt: Future[Statement], session: Session) extends GraphStage[SourceShape[Row]] {
  val out: Outlet[Row] = Outlet("CassandraSource.out")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var maybeRs = Option.empty[ResultSet]
      var futFetchedCallback: AsyncCallback[Try[ResultSet]] = _

      override def preStart(): Unit = {
        implicit val ec = materializer.executionContext

        futFetchedCallback = getAsyncCallback[Try[ResultSet]](tryPushAfterFetch)

        val futRs = futStmt.flatMap(stmt => session.executeAsync(stmt).asScala())
        futRs.onComplete(futFetchedCallback.invoke)
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            implicit val ec = materializer.executionContext

            maybeRs match {
              case Some(rs) if rs.getAvailableWithoutFetching > 0 => push(out, rs.one())
              case Some(rs) if rs.isExhausted => completeStage()
              case Some(rs) =>
                // fetch next page
                val futRs = rs.fetchMoreResults().asScala()
                futRs.onComplete(futFetchedCallback.invoke)
              case None => () // doing nothing, waiting for futRs in preStart() to be completed
            }
          }
        }
      )

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
}
