/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.collection.JavaConverters._

import akka.stream.alpakka.orientdb.impl.OrientDBFlowStage

object OrientDBFlow {

  /**
   * Java API: creates a [[OrientDBFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OrientDbWriteMessage[ODocument, NotUsed], java.util.List[OrientDbWriteMessage[ODocument, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, NotUsed](
          className,
          settings,
          None
        )
      )
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[OrientDBFlowStage]] that accepts specific type
   */
  def typed[T](
      className: String,
      settings: OrientDBUpdateSettings,
      clazz: Option[Class[T]]
  ): Flow[OrientDbWriteMessage[T, NotUsed], java.util.List[OrientDbWriteMessage[T, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[T, NotUsed](className, settings, clazz)
      )
      .map(_.asJava)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow]] from [[OrientDbWriteMessage]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OrientDbWriteMessage[ODocument, C], java.util.List[OrientDbWriteMessage[ODocument, C]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, C](
          className,
          settings,
          None
        )
      )
      .map(_.asJava)
      .asJava
}
