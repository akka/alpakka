/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.recordio.javadsl

import akka.NotUsed
import akka.stream.javadsl.Flow
import akka.util.ByteString

// Provides a JSON framing flow that can separate records from an incoming RecordIO-formatted [[ByteString]] stream.
object RecordIOFraming {

  /**
   * Returns a flow that parses an incoming RecordIO stream and emits the identified records.
   *
   * The incoming stream is expected to be a concatenation of records of the format:
   *
   *   [record length]\n[record data]
   *
   * The parser ignores whitespace before or after each record. It is agnostic to the record data contents.
   *
   * The flow will emit each record's data as a byte string.
   *
   * @param maxRecordLength The maximum record length allowed. If a record is indicated to be longer, this Flow will fail the stream.
   */
  def scanner(maxRecordLength: Int = Int.MaxValue): Flow[ByteString, ByteString, NotUsed] =
    akka.stream.alpakka.recordio.scaladsl.RecordIOFraming.scanner(maxRecordLength).asJava
}
