/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdin

import akka.annotation.InternalApi

import scala.util.{Failure, Success, Try}

/**
 * Used to wrap scala.io.StdIn.readLine() call used by a StdinSource internally. The primary purpose of this trait
 * is to aid unit testing of the stdin module. Cases where users (e.g. not Alpakka developers) would need
 *  to work with this aren't anticipated.
 */
sealed trait StdinSourceReader {
  def read(): Try[Option[String]]
}

/**
 * The deafult unsafe IO reader for StdinSource Sources.
 */
object StdinSourceReaderIo extends StdinSourceReader {

  /**
   * Wrapper for [[scala.io.StdIn.readLine()]].
   * @return A String entered to standard input.
   */
  def read(): Try[Option[String]] = Try(Some(scala.io.StdIn.readLine()))
}

/**
 * An IO safe reader that is initialised with a List[String], whose head is popped and returned on each call
 * to [[read()]]. This constructor is INTERNAL API, but instances for testing can be constructed using
 * [[testkit.ReaderFactory]].
 * @param messages A list of messages to be returned consecutively by calls to [[read()]].
 */
@InternalApi
private[stdin] final class StdinSourceReaderFromList(messages: List[String]) extends StdinSourceReader {

  private var msgStack: List[String] = messages

  def read(): Try[Option[String]] = msgStack match {
    case x :: xs => Try { msgStack = xs; Some(x) }
    case Nil => Success(None)
  }
}

/**
 * A reader that returns a set [[Failure()]] on a call to [[read()]]. This constructor is INTERNAL API,
 * but instances for testing can be constructed using [[testkit.ReaderFactory]].
 */
@InternalApi
private[stdin] final class StdinSourceReaderThrowsException() extends StdinSourceReader {

  def read(): Try[Option[String]] = Failure { new java.util.NoSuchElementException("example reader failure") }
}
