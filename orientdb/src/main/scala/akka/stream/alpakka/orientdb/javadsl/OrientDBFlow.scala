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
  ): Flow[OIncomingMessage[ODocument, NotUsed], java.util.List[OIncomingMessage[ODocument, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, NotUsed, java.util.List[OIncomingMessage[ODocument, NotUsed]]](
          className,
          settings,
          _.asJava,
          None
        )
      )
      .asJava

  /**
   * Java API: creates a [[OrientDBFlowStage]] that accepts specific type
   */
  def typed[T](
      className: String,
      settings: OrientDBUpdateSettings,
      clazz: Option[Class[T]]
  ): Flow[OIncomingMessage[T, NotUsed], java.util.List[OIncomingMessage[T, NotUsed]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[T, NotUsed, java.util.List[OIncomingMessage[T, NotUsed]]](className,
                                                                                        settings,
                                                                                        _.asJava,
                                                                                        clazz)
      )
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow]] from [[OIncomingMessage]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDBUpdateSettings
  ): Flow[OIncomingMessage[ODocument, C], java.util.List[OIncomingMessage[ODocument, C]], NotUsed] =
    akka.stream.scaladsl.Flow
      .fromGraph(
        new OrientDBFlowStage[ODocument, C, java.util.List[OIncomingMessage[ODocument, C]]](
          className,
          settings,
          _.asJava,
          None
        )
      )
      .asJava
}
