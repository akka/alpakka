/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import org.mongodb.scala.bson.conversions.Bson

/**
 *
 * @param filter a document describing the query filter, which may not be null. This can be of any type for which a { @code Codec} is
 *               registered
 * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
 *               can be of any type for which a { @code Codec} is registered
 */
final case class DocumentUpdate(filter: Bson, update: Bson)
