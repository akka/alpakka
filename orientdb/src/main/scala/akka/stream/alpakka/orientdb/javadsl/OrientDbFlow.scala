/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.collection.JavaConverters._

import akka.stream.alpakka.orientdb.impl.OrientDbFlowStage

object OrientDbFlow {

  /**
   * Java API: creates a [[OrientDbFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDbWriteSettings
  ): Flow[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]],
          java.util.List[OrientDbWriteMessage[ODocument, NotUsed]],
          NotUsed] =
    akka.stream.javadsl.Flow
      .of(classOf[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]]])
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.create(className, settings))
      .map(_.asJava)

  /**
   * Java API: creates a [[OrientDbFlowStage]] that accepts specific type
   */
  def typed[T](
      className: String,
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Flow[java.util.List[OrientDbWriteMessage[T, NotUsed]], java.util.List[OrientDbWriteMessage[T, NotUsed]], NotUsed] =
    akka.stream.javadsl.Flow
      .of(classOf[java.util.List[OrientDbWriteMessage[T, NotUsed]]])
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.typed[T](className, settings, clazz))
      .map(_.asJava)

  /**
   * Creates a [[akka.stream.javadsl.Flow]] from [[OrientDbWriteMessage]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDbWriteSettings
  ): Flow[java.util.List[OrientDbWriteMessage[ODocument, C]],
          java.util.List[OrientDbWriteMessage[ODocument, C]],
          NotUsed] =
    akka.stream.javadsl.Flow
      .of(classOf[java.util.List[OrientDbWriteMessage[ODocument, C]]])
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.createWithPassThrough[C](className, settings))
      .map(_.asJava)
}
