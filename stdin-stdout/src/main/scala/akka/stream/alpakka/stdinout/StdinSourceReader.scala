/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout

import akka.annotation.InternalApi

import scala.util.{Failure, Success, Try}

/**
 * Used to wrap scala.io.StdIn.readLine() call used by a StdinSource internally. Primary purpose is to
 *  enable unit testing of the module.
 */
sealed trait StdinSourceReader {
  def read(): Try[Option[String]]
}

/**
 * The unsafe IO reader that is the default for StdinSources. Part of the public API as it is defaulted
 * to in the Source builders, but cases where users (not developers) would need to work with this aren't
 * anticipated.
 */
object StdinSourceReaderIo extends StdinSourceReader {

  /**
   * Alias for [[scala.io.StdIn.readLine()]].
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
private[stdinout] final class StdinSourceReaderFromList(messages: List[String]) extends StdinSourceReader {

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
private[stdinout] final class StdinSourceReaderThrowsException() extends StdinSourceReader {

  def read(): Try[Option[String]] = Failure { new java.util.NoSuchElementException("example reader failure") }
}
