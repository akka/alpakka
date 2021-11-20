/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import akka.NotUsed

object OrientDbWriteMessage {
  // Apply method to use when not using passThrough
  def apply[T](oDocument: T): OrientDbWriteMessage[T, NotUsed] =
    OrientDbWriteMessage(oDocument, NotUsed)

  // Java-api - without passThrough
  def create[T](oDocument: T): OrientDbWriteMessage[T, NotUsed] =
    OrientDbWriteMessage(oDocument, NotUsed)

  // Java-api - with passThrough
  def create[T, C](oDocument: T, passThrough: C) =
    OrientDbWriteMessage(oDocument, passThrough)
}

final case class OrientDbWriteMessage[T, C](oDocument: T, passThrough: C)

final case class OrientDbReadResult[T](oDocument: T)
