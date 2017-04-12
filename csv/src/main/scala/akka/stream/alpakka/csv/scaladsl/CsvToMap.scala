/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.stream.alpakka.csv.CsvToMapStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString

object CsvToMap {

  /**
   * A flow translating incoming [[List]] of [[ByteString]] to a [[Map[String, ByteString]]] using the streams first
   * element's values as keys.
   * @param charset the charset to decode [[ByteString]] to [[String]], defaults to UTF-8
   */
  def toMap(charset: Charset = StandardCharsets.UTF_8): Flow[List[ByteString], Map[String, ByteString], NotUsed] =
    Flow.fromGraph(new CsvToMapStage(columnNames = None, charset))

  /**
   * A flow translating incoming [[List[ByteString]]] to a [[Map[String, ByteString]]] using the given headers
   * as keys.
   * @param headers column names to be used as map keys
   */
  def withHeaders(headers: String*): Flow[List[ByteString], Map[String, ByteString], NotUsed] =
    Flow.fromGraph(new CsvToMapStage(Some(headers.toList), StandardCharsets.UTF_8))
}
