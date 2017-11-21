/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.orientdb._
import akka.stream.scaladsl.{Keep, Sink}
import com.orientechnologies.orient.core.record.impl.ODocument

import scala.concurrent.Future

object OrientDBSink {

  /**
   * Scala API: creates a sink that accepts as ODocument
   */
  def apply(className: String,
            settings: OrientDBUpdateSettings): Sink[OIncomingMessage[ODocument, NotUsed], Future[Done]] =
    OrientDBFlow.create(className, settings).toMat(Sink.ignore)(Keep.right)

}
