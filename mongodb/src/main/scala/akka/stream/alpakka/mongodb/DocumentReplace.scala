/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb

import org.bson.conversions.Bson

case class DocumentReplace[T](filter: Bson, replacement: T)
