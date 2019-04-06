/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdin.javadsl

import akka.NotUsed
import akka.stream.alpakka.stdin
import akka.stream.alpakka.stdin.{StdinSourceReader, StdinSourceReaderIo}
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
    stdin.scaladsl.StdinSource.create(reader).asJava

}
