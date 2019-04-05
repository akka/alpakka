/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.alpakka.stdinout.{impl, StdinSourceReader, StdinSourceReaderIo}

/**
 * Scala API to create StdinSource sources.
 */
object StdinSource {

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from standard input that streams String values.
   * Alias of [[create()]].
   * @param reader A control object for reading of Strings, configurable largely for testing, it's unlikely users
   *               need to change this from the default [[akka.stream.alpakka.stdinout.StdinSourceReaderIo]] value
   * @return A runnable StdinSource instance.
   */
  def apply(reader: StdinSourceReader = StdinSourceReaderIo): Source[String, NotUsed] = create(reader)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from standard inout that streams Strings.
   * @param reader A control object for reading of Strings, configurable largely for testing, it's unlikely users
   *               need to change this from the default [[akka.stream.alpakka.stdinout.StdinSourceReaderIo]] value.
   * @return A runnable StdinSource instance.
   */
  def create(reader: StdinSourceReader = StdinSourceReaderIo): Source[String, NotUsed] =
    Source.fromGraph(new impl.StdinSource(reader))

}
