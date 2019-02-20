/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.recordio.scaladsl

import akka.NotUsed
import akka.stream.alpakka.recordio.impl.RecordIOFramingStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString

/**
 * Scala API
 *
 * Provides a flow that can separate records from an incoming RecordIO-formatted [[akka.util.ByteString]] stream.
 */
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
    Flow[ByteString].via(new RecordIOFramingStage(maxRecordLength)).named("recordIOFraming")
}
