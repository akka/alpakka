/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.querybuilder.Select
import com.datastax.driver.core.{ResultSet, Row, Session}
import GuavaFutures._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * Wrap result of CQL select to [[akka.stream.stage.GraphStage#GraphStage()]]
 *
 * Repeat select if [[com.datastax.driver.core.ResultSet]] with cached `lastSelectedId` is empty.
 *
 * <ul> Main steps:
 * <li> make select </li>
 * <li> fetch row if [[com.datastax.driver.core.ResultSet]] hasn't available row </li>
 * <li> make select by saved `lastSelectedId` if [[com.datastax.driver.core.ResultSet]] isn't exhausted </li>
 * <li> save `lastSelectedId` of row and push it </li>
 * </ul>
 *
 * @param afterId      id of last selected row. Used as `where id < afterId`
 * @param statement    function for creation select statement by given afterId
 * @param getId        function for get id from [[com.datastax.driver.core.Row]]
 * @param fetchSize    the fetch size to use. If  fetchSize <= 0 ,
 *                     the default fetch size will be used. To disable paging of the
 *                     result set, use fetchSize == Integer.MAX_VALUE.
 * @param batchSize    number of rows getted by each select.
 * @param session      [[com.datastax.driver.core.Session]] to Cassandra
 * @param pollInterval interval for selecting new row if ResultSet is exhausted
 * @tparam KEY type of primary key of in [[com.datastax.driver.core.Row]]
 */
final class CassandraRereadableSourceStage[KEY](afterId: KEY,
                                                statement: KEY => Select.Where,
                                                getId: Row => KEY,
                                                fetchSize: Int,
                                                batchSize: Int,
                                                session: Session,
                                                pollInterval: FiniteDuration,
                                                system: ActorSystem)
    extends GraphStage[SourceShape[Row]] {

  val out: Outlet[Row] = Outlet("CassandraRereadableSource.out")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var maybeResultSet = Option.empty[ResultSet]
      private var futFetchedCallback: AsyncCallback[Try[ResultSet]] = _
      private var lastSelectId: KEY = afterId
      private var lastRunningWithDelay: Option[Cancellable] = None

      private def runWithDelay(delay: FiniteDuration)(f: => Unit): Unit = {
        if (lastRunningWithDelay.nonEmpty) {
          lastRunningWithDelay.foreach(_.cancel())
        }

        schedulerOnce(delay, f)
      }

      private def schedulerOnce(delay: FiniteDuration, f: => Unit): Unit = {
        val cancellable = system.scheduler.scheduleOnce(delay) {
          lastRunningWithDelay = None // remove link to last running with delay
          f // run new callback
        }(materializer.executionContext)

        lastRunningWithDelay = Some(cancellable)
      }

      override def preStart(): Unit = {
        futFetchedCallback = getAsyncCallback[Try[ResultSet]](tryPushAfterFetch)
        selectMore()
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            implicit val ec: ExecutionContextExecutor = materializer.executionContext

            maybeResultSet match {
              case Some(resultSet) if resultSet.getAvailableWithoutFetching > 0 =>
                pushOne(resultSet)

              case Some(rs) if rs.isExhausted =>
                selectMore()

              case Some(rs) =>
                val futureResultSet = rs.fetchMoreResults().asScala()
                futureResultSet.onComplete(futFetchedCallback.invoke)
              case None => () // doing nothing, waiting for futRs in preStart() to be completed
            }
          }
        }
      )

      override def postStop(): Unit = {
        if (lastRunningWithDelay.nonEmpty) {
          lastRunningWithDelay.foreach(_.cancel())
        }
        super.postStop()
      }

      private def selectMore(): Unit =
        if (maybeResultSet.isEmpty || maybeResultSet.exists(_.getAvailableWithoutFetching < 1)) {

          implicit val ec: ExecutionContextExecutor = materializer.executionContext
          val selectStatement = statement(lastSelectId).limit(batchSize).setFetchSize(fetchSize)

          val futureResultSet = session.executeAsync(selectStatement).asScala()
          futureResultSet.onComplete(futFetchedCallback.invoke)

        } else {
          runWithDelay(pollInterval)(selectMore())
        }

      private def tryPushAfterFetch(rsOrFailure: Try[ResultSet]): Unit = {
        implicit val ec: ExecutionContextExecutor = materializer.executionContext

        rsOrFailure match {
          case Success(rs) =>
            maybeResultSet = Some(rs)
            if (rs.getAvailableWithoutFetching > 0) {
              pushOne(rs)
            } else {
              runWithDelay(pollInterval)(selectMore())
            }

          case Failure(_: NoHostAvailableException) =>
            maybeResultSet = None
            runWithDelay(pollInterval)(selectMore())

          case Failure(failure) =>
            failStage(failure)
        }
      }

      private def pushOne(resultSet: ResultSet): Unit =
        if (isAvailable(out)) {
          val row = resultSet.one()
          lastSelectId = getId(row)
          push(out, row)
        } else {
          runWithDelay(pollInterval)(pushOne(resultSet))
        }
    }
}
