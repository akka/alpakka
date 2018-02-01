/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.collection.JavaConverters._
import java.util.{List => JavaList}

object OrientDBFlow {

  /**
   * Java API: creates a [[OrientDBFlowStage]] that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OIncomingMessage[ODocument, NotUsed], JavaList[OIncomingMessage[ODocument, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, NotUsed, JavaList[OIncomingMessage[ODocument, NotUsed]]](
          className,
          settings,
          _.asJava,
          None
        )
      )
      .mapAsync(1)(identity)
      .asJava

  /**
   * Java API: creates a [[OrientDBFlowStage]] that accepts specific type
   */
  def typed[T](
      className: String,
      settings: OrientDBUpdateSettings,
      clazz: Option[Class[T]]
  ): Flow[OIncomingMessage[T, NotUsed], JavaList[OIncomingMessage[T, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[T, NotUsed, JavaList[OIncomingMessage[T, NotUsed]]](className, settings, _.asJava, clazz)
      )
      .mapAsync(1)(identity)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow]] from [[OIncomingMessage]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OIncomingMessage[ODocument, C], JavaList[OIncomingMessage[ODocument, C]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, C, JavaList[OIncomingMessage[ODocument, C]]](
          className,
          settings,
          _.asJava,
          None
        )
      )
      .mapAsync(1)(identity)
      .asJava
}
