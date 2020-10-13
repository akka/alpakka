/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl.Flow
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.collection.JavaConverters._

/**
 * Java API.
 */
object OrientDbFlow {

  /**
   * Flow to write `ODocument`s to OrientDB, elements within one list are stored within one transaction.
   */
  def create(
      className: String,
      settings: OrientDbWriteSettings
  ): Flow[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]], java.util.List[OrientDbWriteMessage[ODocument,
                                                                                                        NotUsed
  ]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]]]
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.create(className, settings))
      .map(_.asJava)
      .asJava

  /**
   * Flow to write `ODocument`s to OrientDB, elements within one list are stored within one transaction.
   * Allows a `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      className: String,
      settings: OrientDbWriteSettings
  ): Flow[java.util.List[OrientDbWriteMessage[ODocument, C]],
          java.util.List[OrientDbWriteMessage[ODocument, C]],
          NotUsed
  ] =
    akka.stream.scaladsl
      .Flow[java.util.List[OrientDbWriteMessage[ODocument, C]]]
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.createWithPassThrough[C](className, settings))
      .map(_.asJava)
      .asJava

  /**
   * Flow to write elements of type `T` to OrientDB, elements within one list are stored within one transaction.
   */
  def typed[T](
      className: String,
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Flow[java.util.List[OrientDbWriteMessage[T, NotUsed]], java.util.List[OrientDbWriteMessage[T, NotUsed]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[OrientDbWriteMessage[T, NotUsed]]]
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.typed[T](className, settings, clazz))
      .map(_.asJava)
      .asJava

  /**
   * Flow to write elements of type `T` to OrientDB, elements within one list are stored within one transaction.
   * Allows a `passThrough` of type `C`.
   */
  def typedWithPassThrough[T, C](
      className: String,
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Flow[java.util.List[OrientDbWriteMessage[T, C]], java.util.List[OrientDbWriteMessage[T, C]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[OrientDbWriteMessage[T, C]]]
      .map(_.asScala.toList)
      .via(scaladsl.OrientDbFlow.typedWithPassThrough[T, C](className, settings, clazz))
      .map(_.asJava)
      .asJava
}
