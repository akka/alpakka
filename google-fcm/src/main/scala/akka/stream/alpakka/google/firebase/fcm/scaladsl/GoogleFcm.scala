/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.stream.alpakka.google.firebase.fcm.impl.FcmFlows
import akka.stream.alpakka.google.firebase.fcm.{FcmNotification, FcmResponse, FcmSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.github.ghik.silencer.silent

import scala.concurrent.Future

@silent("deprecated")
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
