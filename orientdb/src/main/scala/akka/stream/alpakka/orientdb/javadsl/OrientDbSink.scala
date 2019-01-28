/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.orientdb.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.orientdb._
import akka.stream.javadsl._
import com.orientechnologies.orient.core.record.impl.ODocument

object OrientDbSink {

  /**
   * Java API: creates a sink that accepts as ODocument
   */
  def create(
      className: String,
      settings: OrientDbWriteSettings
  ): Sink[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]], CompletionStage[Done]] =
    OrientDbFlow
      .create(className, settings)
      .toMat(Sink.ignore[java.util.List[OrientDbWriteMessage[ODocument, NotUsed]]],
             Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a sink that accepts as specific type
   */
  def typed[T](className: String,
               settings: OrientDbWriteSettings,
               clazz: Class[T]): Sink[java.util.List[OrientDbWriteMessage[T, NotUsed]], CompletionStage[Done]] =
    OrientDbFlow
      .typed[T](className, settings, clazz)
      .toMat(Sink.ignore[java.util.List[OrientDbWriteMessage[T, NotUsed]]], Keep.right[NotUsed, CompletionStage[Done]])
}
