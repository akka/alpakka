/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.alpakka.orientdb.impl.OrientDbFlowStage
import akka.stream.scaladsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.immutable

/**
 * Scala API.
 */
object OrientDbFlow {

  /**
   * Flow to write `ODocument`s to OrientDB, elements within one list are stored within one transaction.
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
   * Flow to write `ODocument`s to OrientDB, elements within one sequence are stored within one transaction.
   * Allows a `passThrough` of type `C`.
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
   * Flow to write elements of type `T` to OrientDB, elements within one sequence are stored within one transaction.
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

  /**
   * Flow to write elements of type `T` to OrientDB, elements within one sequence are stored within one transaction.
   * Allows a `passThrough` of type `C`.
   */
  def typedWithPassThrough[T, C](
      className: String,
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Flow[immutable.Seq[OrientDbWriteMessage[T, C]], immutable.Seq[OrientDbWriteMessage[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new OrientDbFlowStage[T, C](
          className,
          settings,
          Some(clazz)
        )
      )
}
