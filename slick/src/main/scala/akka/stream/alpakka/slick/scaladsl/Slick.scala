/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.slick.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.NotUsed

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import slick.dbio.DBIO
import slick.dbio.StreamingDBIO

/**
 * Methods for interacting with relational databases using Slick and akka-stream.
 */
object Slick {

  /**
   * Scala API: creates a Source[T, NotUsed] that performs the
   *            specified query against the (implicitly) specified
   *            Slick database and streams the results.
   *            This works for both "typed" Slick queries
   *            and "plain SQL" queries.
   *
   * @param streamingQuery The Slick query to execute, which can
   *                       be either a "typed" query or a "plain SQL"
   *                       query produced by one of the Slick "sql..."
   *                       String interpolators
   * @param session The database session to use.
   */
  def source[T](
      streamingQuery: StreamingDBIO[Seq[T], T]
  )(implicit session: SlickSession): Source[T, NotUsed] =
    Source.fromPublisher(session.db.stream(streamingQuery))

  /**
   * Scala API: creates a Flow that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, and then executes
   *            those statements against the specified Slick database.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param session The database session to use.
   */
  def flow[T](
      toStatement: T => DBIO[Int]
  )(implicit session: SlickSession): Flow[T, Int, NotUsed] = flow(1, toStatement)

  /**
   * Scala API: creates a Flow that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, and then executes
   *            those statements against the specified Slick database.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param parallelism How many parallel asynchronous streams should be
   *                    used to send statements to the database. Use a
   *                    value of 1 for sequential execution.
   * @param session The database session to use.
   */
  def flow[T](
      parallelism: Int,
      toStatement: T => DBIO[Int]
  )(implicit session: SlickSession): Flow[T, Int, NotUsed] =
    flowWithPassThrough(parallelism, toStatement)

  /**
   * Scala API: creates a Flow that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, then executes
   *            those statements against the specified Slick database
   *            and returns the statement result type R.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param session The database session to use.
   */
  def flowWithPassThrough[T, R](
      toStatement: T => DBIO[R]
  )(implicit session: SlickSession): Flow[T, R, NotUsed] = flowWithPassThrough(1, toStatement)

  /**
   * Scala API: creates a Flow that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, then executes
   *            those statements against the specified Slick database
   *            and returns the statement result type R.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param parallelism How many parallel asynchronous streams should be
   *                    used to send statements to the database. Use a
   *                    value of 1 for sequential execution.
   * @param session The database session to use.
   */
  def flowWithPassThrough[T, R](
      parallelism: Int,
      toStatement: T => DBIO[R]
  )(implicit session: SlickSession): Flow[T, R, NotUsed] =
    Flow[T]
      .mapAsync(parallelism) { t =>
        session.db.run(toStatement(t))
      }

  /**
   * Scala API: creates a Sink that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, and then executes
   *            those statements against the specified Slick database.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param session The database session to use.
   */
  def sink[T](
      toStatement: T => DBIO[Int]
  )(implicit session: SlickSession): Sink[T, Future[Done]] =
    flow[T](1, toStatement).toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a Sink that takes a stream of elements of
   *            type T, transforms each element to a SQL statement
   *            using the specified function, and then executes
   *            those statements against the specified Slick database.
   *
   * @param toStatement A function to produce the SQL statement to
   *                    execute based on the current element.
   * @param parallelism How many parallel asynchronous streams should be
   *                    used to send statements to the database. Use a
   *                    value of 1 for sequential execution.
   * @param session The database session to use.
   */
  def sink[T](
      parallelism: Int,
      toStatement: T => DBIO[Int]
  )(implicit session: SlickSession): Sink[T, Future[Done]] =
    flow[T](parallelism, toStatement).toMat(Sink.ignore)(Keep.right)
}
