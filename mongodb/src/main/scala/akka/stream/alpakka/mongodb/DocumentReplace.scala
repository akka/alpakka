/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb

import org.bson.conversions.Bson

/**
 * @param filter      a document describing the query filter, which may not be null. This can be of any type for which a { @code Codec} is registered
 * @param replacement an object to replace the previous one, which may not be null. This can be of any type for which a { @code Codec} is registered
 */
final class DocumentReplace[T] private (val filter: Bson, val replacement: T) {

  def withFilter(filter: Bson): DocumentReplace[T] = copy(filter = filter)
  def withReplacement[T1](replacement: T1): DocumentReplace[T1] = copy(replacement = replacement)

  override def toString: String =
    "DocumentReplace(" +
    s"filter=$filter," +
    s"replacement=$replacement" +
    ")"

  private def copy[T1](filter: Bson = filter, replacement: T1 = replacement) =
    new DocumentReplace[T1](filter, replacement)
}

object DocumentReplace {
  def apply[T](filter: Bson, replacement: T): DocumentReplace[T] = new DocumentReplace(filter, replacement)

  /**
   * Java Api
   */
  def create[T](filter: Bson, replacement: T): DocumentReplace[T] = DocumentReplace(filter, replacement)
}
