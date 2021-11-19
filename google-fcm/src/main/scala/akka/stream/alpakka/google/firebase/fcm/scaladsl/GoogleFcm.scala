/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.stream.alpakka.google.firebase.fcm.impl.FcmFlows
import akka.stream.alpakka.google.firebase.fcm.{FcmNotification, FcmResponse, FcmSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object GoogleFcm {

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm", "3.0.2")
  @Deprecated
  def sendWithPassThrough[T](conf: FcmSettings): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    FcmFlows.fcmWithData[T](conf)

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm", "3.0.2")
  @Deprecated
  def send(conf: FcmSettings): Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf)

  /** Use akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm */
  @deprecated("akka.stream.alpakka.google.firebase.fcm.v1.scaladsl.GoogleFcm", "3.0.2")
  @Deprecated
  def fireAndForget(conf: FcmSettings): Sink[FcmNotification, Future[Done]] =
    FcmFlows.fcm(conf).toMat(Sink.ignore)(Keep.right)

}
