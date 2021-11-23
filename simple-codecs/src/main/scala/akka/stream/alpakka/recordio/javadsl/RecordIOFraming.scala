/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.recordio.javadsl

import akka.NotUsed
import akka.stream.alpakka.recordio.impl.RecordIOFramingStage
import akka.stream.javadsl.Flow
import akka.util.ByteString

/**
 * Java API
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
  def scanner(maxRecordLength: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph(new RecordIOFramingStage(maxRecordLength)).named("recordIOFraming")

  def scanner(): Flow[ByteString, ByteString, NotUsed] = scanner(10 * 1024)
}
