/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb
import org.bson.conversions.Bson

/**
 *
 * @param filter a document describing the query filter, which may not be null. This can be of any type for which a { @code Codec} is
 *               registered
 * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
 *               can be of any type for which a { @code Codec} is registered
 */
final class DocumentUpdate private (val filter: Bson, val update: Bson) {

  def withFilter(filter: Bson): DocumentUpdate = copy(filter = filter)
  def withUpdate(update: Bson): DocumentUpdate = copy(update = update)

  override def toString: String =
    "DocumentUpdate(" +
    s"filter=$filter," +
    s"update=$update" +
    ")"

  private def copy(filter: Bson = filter, update: Bson = update) =
    new DocumentUpdate(filter, update)
}

object DocumentUpdate {
  def apply(filter: Bson, update: Bson) = new DocumentUpdate(filter, update)

  /**
   * Java Api
   */
  def create(filter: Bson, update: Bson) = DocumentUpdate(filter, update)
}
