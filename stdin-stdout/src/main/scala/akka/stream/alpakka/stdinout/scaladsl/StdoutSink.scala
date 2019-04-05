/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.alpakka.stdinout.impl

/**
 * Scala API to create StdoutSink sinks.
 */
object StdoutSink {

  /**
   * Creates a [[akka.stream.scaladsl.Sink]] that prints incoming String values to stdout.
   * Alias of [[create()]].
   * @return A runnable StdoutSink instance.
   */
  def apply(): Sink[String, NotUsed] = create()

  /**
   * Creates a [[akka.stream.scaladsl.Sink]] that prints incoming String values to stdout.
   * @return A runnable StdoutSink instance.
   */
  def create(): Sink[String, NotUsed] = Sink.fromGraph(new impl.StdoutSink)

}
