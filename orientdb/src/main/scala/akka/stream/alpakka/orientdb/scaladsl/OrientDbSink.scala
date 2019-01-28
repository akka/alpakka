/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.orientdb._
import akka.stream.scaladsl.{Keep, Sink}
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.concurrent.Future
import scala.collection.immutable

object OrientDbSink {

  /**
   * Scala API: creates a sink that accepts as ODocument
   */
  def apply(
      className: String,
      settings: OrientDbWriteSettings
  ): Sink[immutable.Seq[OrientDbWriteMessage[ODocument, NotUsed]], Future[Done]] =
    OrientDbFlow.create(className, settings).toMat(Sink.ignore)(Keep.right)

}
