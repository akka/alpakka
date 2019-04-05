/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.javadsl

import akka.NotUsed
import akka.stream.alpakka.stdinout
import akka.stream.javadsl.Sink

/**
 * Java API to create StdoutSink Sinks.
 */
object StdoutSink {

  /**
   * No Java API at the start of the method doc needed, since the package is dedicated to the Java API.
   *
   * Call Scala source factory and convert both: the source and materialized values to Java classes.
   */
  def create(): Sink[String, NotUsed] =
    stdinout.scaladsl.StdoutSink.create().asJava

}
