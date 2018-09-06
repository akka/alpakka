/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb

import akka.NotUsed

object OIncomingMessage {
  // Apply method to use when not using passThrough
  def apply[T](oDocument: T): OIncomingMessage[T, NotUsed] =
    OIncomingMessage(oDocument, NotUsed)

  // Java-api - without passThrough
  def create[T](oDocument: T): OIncomingMessage[T, NotUsed] =
    OIncomingMessage(oDocument, NotUsed)

  // Java-api - with passThrough
  def create[T, C](oDocument: T, passThrough: C) =
    OIncomingMessage(oDocument, passThrough)
}

final case class OIncomingMessage[T, C](oDocument: T, passThrough: C)

final case class OOutgoingMessage[T](oDocument: T)

case class OSQLResponse[T](error: Option[String], result: Seq[OOutgoingMessage[T]])
