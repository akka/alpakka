/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.{WriteMessage, WriteResult}

import scala.collection.immutable

/**
 * Internal API.
 */
@InternalApi
private[impl] abstract class RestBulkApi[T, C] {
  def toJson(messages: immutable.Seq[WriteMessage[T, C]]): String

  def toWriteResults(messages: immutable.Seq[WriteMessage[T, C]], jsonString: String): immutable.Seq[WriteResult[T, C]]
}
