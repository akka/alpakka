/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.javadsl

import akka.NotUsed
import akka.stream.alpakka.stdinout
import akka.stream.alpakka.stdinout.{StdinSourceReader, StdinSourceReaderIo}
import akka.stream.javadsl.Source

/**
 * Java API to create StdinSource Sources.
 */
object StdinSource {

  /**
   * No Java API at the start of the method doc needed, since the package is dedicated to the Java API.
   *
   * Call Scala source factory and convert both: the source and materialized values to Java classes.
   */
  def create(reader: StdinSourceReader = StdinSourceReaderIo): Source[String, NotUsed] =
    stdinout.scaladsl.StdinSource.create(reader).asJava

}
