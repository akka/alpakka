/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.orientdb._
import akka.stream.scaladsl.{Keep, Sink}
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.concurrent.Future
import scala.collection.immutable

/**
 * Scala API.
 */
object OrientDbSink {

  /**
   * Sink to write `ODocument`s to OrientDB, elements within one sequence are stored within one transaction.
   */
  def apply(
      className: String,
      settings: OrientDbWriteSettings
  ): Sink[immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]], Future[Done]] =
    OrientDbFlow.create(className, settings).toMat(Sink.ignore)(Keep.right)

  /**
   * Flow to write elements of type `T` to OrientDB, elements within one sequence are stored within one transaction.
   */
  def typed[T](
      className: String,
      settings: OrientDbWriteSettings,
      clazz: Class[T]
  ): Sink[immutable.Seq[OrientDbWriteMessage[T, NotUsed]], Future[Done]] =
    OrientDbFlow
      .typed(className, settings, clazz)
      .toMat(Sink.ignore)(Keep.right)

}
