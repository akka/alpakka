/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.testkit

import akka.annotation.ApiMayChange
import akka.stream.alpakka.stdinout.{StdinSourceReader, StdinSourceReaderFromList, StdinSourceReaderThrowsException}

object ReaderFactory {

  /**
   * Scala API
   * Java API
   * Creates an IO safe reader object for testing.
   * @param messages List of messages to be returned on calls to read().
   * @return An IO safe reader object, with initialised messages.
   */
  @ApiMayChange
  def createStdinSourceReaderFromList(messages: Array[String]): StdinSourceReader =
    new StdinSourceReaderFromList(messages = messages.toList)

  /**
   * Scala API
   * Java API
   * Creates a reader object that throws an exception for testing.
   * @return A object that will throw an exception when read() is called on it.
   */
  @ApiMayChange
  def createStdinSourceReaderThrowsException(): StdinSourceReader =
    new StdinSourceReaderThrowsException

}
