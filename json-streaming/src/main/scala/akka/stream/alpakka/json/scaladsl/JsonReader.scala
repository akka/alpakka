/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.json.scaladsl

import akka.NotUsed
import akka.stream.alpakka.json.impl.JsonStreamReader
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.jsfr.json.compiler.JsonPathCompiler
import org.jsfr.json.path.JsonPath

object JsonReader {

  /**
   * A Flow that consumes incoming json in chunks and produces a stream of parsable json values
   * according to the JsonPath given.
   *
   * JsonPath examples:
   * - Stream all elements of the nested array `rows`: `$.rows[*]`
   * - Stream the value of `doc` of each element in the array: `$.rows[*].doc`
   *
   * Supported JsonPath syntax: https://github.com/jsurfer/JsonSurfer#what-is-jsonpath
   */
  def select(path: JsonPath): Flow[ByteString, ByteString, NotUsed] = Flow.fromGraph(new JsonStreamReader(path))

  /**
   * A Flow that consumes incoming json in chunks and produces a stream of parsable json values
   * according to the JsonPath given. The passed String will need to be parsed first.
   *
   * @see [[#select]]
   */
  def select(path: String): Flow[ByteString, ByteString, NotUsed] = select(JsonPathCompiler.compile(path))
}
