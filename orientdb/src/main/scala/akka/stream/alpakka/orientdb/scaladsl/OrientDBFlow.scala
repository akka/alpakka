/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.OrientDBFlowStage
import akka.stream.scaladsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.immutable

object OrientDBFlow {

  /**
   * Scala API: creates a [[OrientDBFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]],
          immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]],
          NotUsed] =
    Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, NotUsed](
          className,
          settings,
          None
        )
      )

  /**
   * Creates a [[akka.stream.scaladsl.Flow]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[immutable.Seq[OrientDbWriteMessage[ODocument, C]],
          immutable.Seq[OrientDbWriteMessage[ODocument, C]],
          NotUsed] =
    Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, C](
          className,
          settings,
          None
        )
      )

  /**
   *
   */
  def typed[T](
      className: String,
      settings: OrientDBUpdateSettings,
      clazz: Class[T]
  ): Flow[immutable.Seq[OrientDbWriteMessage[T, NotUsed]], immutable.Seq[OrientDbWriteMessage[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new OrientDBFlowStage[T, NotUsed](
          className,
          settings,
          Some(clazz)
        )
      )

}
