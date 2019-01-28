/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.OrientDbFlowStage
import akka.stream.scaladsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.immutable

object OrientDbFlow {

  /**
   * Scala API: creates a [[OrientDbFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDbWriteSettings
  ): Flow[immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]],
          immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]],
          NotUsed] =
    Flow
      .fromGraph(
        new OrientDbFlowStage[ODocument, NotUsed](
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
      settings: OrientDbWriteSettings
  ): Flow[immutable.Seq[OrientDbWriteMessage[ODocument, C]],
          immutable.Seq[OrientDbWriteMessage[ODocument, C]],
          NotUsed] =
    Flow
      .fromGraph(
        new OrientDbFlowStage[ODocument, C](
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
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Flow[immutable.Seq[OrientDbWriteMessage[T, NotUsed]], immutable.Seq[OrientDbWriteMessage[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new OrientDbFlowStage[T, NotUsed](
          className,
          settings,
          Some(clazz)
        )
      )

}
