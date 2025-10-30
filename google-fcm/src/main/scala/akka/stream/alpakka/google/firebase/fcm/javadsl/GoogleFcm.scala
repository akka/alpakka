/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.firebase.fcm.javadsl

import akka.japi.Pair
import akka.stream.alpakka.google.firebase.fcm.impl.FcmFlows
import akka.stream.alpakka.google.firebase.fcm.{FcmNotification, FcmResponse, FcmSettings}
import akka.stream.{javadsl, scaladsl}
import akka.{Done, NotUsed}

import java.util.concurrent.CompletionStage

object GoogleFcm {

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm" */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm", "3.0.2")
  @Deprecated
  def sendWithPassThrough[T](conf: FcmSettings): javadsl.Flow[Pair[FcmNotification, T], Pair[FcmResponse, T], NotUsed] =
    scaladsl
      .Flow[Pair[FcmNotification, T]]
      .map(_.toScala)
      .via(FcmFlows.fcmWithData[T](conf))
      .map(t => Pair(t._1, t._2))
      .asJava

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm" */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm", "3.0.2")
  @Deprecated
  def send(conf: FcmSettings): javadsl.Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf).asJava

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm" */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.javadsl.GoogleFcm", "3.0.2")
  @Deprecated
  def fireAndForget(conf: FcmSettings): javadsl.Sink[FcmNotification, CompletionStage[Done]] =
    send(conf)
      .toMat(javadsl.Sink.ignore(), javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
