/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.scaladsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument

object OrientDBFlow {

  /**
   * Scala API: creates a [[OrientDBFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OIncomingMessage[ODocument, NotUsed], Seq[OIncomingMessage[ODocument, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, NotUsed, Seq[OIncomingMessage[ODocument, NotUsed]]](
          className,
          settings,
          identity,
          None
        )
      )
      .mapAsync(1)(identity)

  /**
   * Creates a [[akka.stream.scaladsl.Flow]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OIncomingMessage[ODocument, C], Seq[OIncomingMessage[ODocument, C]], NotUsed] =
    Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, C, Seq[OIncomingMessage[ODocument, C]]](
          className,
          settings,
          identity,
          None
        )
      )
      .mapAsync(1)(identity)
}
