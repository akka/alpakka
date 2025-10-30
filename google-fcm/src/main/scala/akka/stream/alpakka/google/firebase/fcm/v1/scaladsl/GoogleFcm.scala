/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.scaladsl

import akka.stream.alpakka.google.firebase.fcm.FcmSettings
import akka.stream.alpakka.google.firebase.fcm.v1.impl.FcmFlows
import akka.stream.alpakka.google.firebase.fcm.v1.models.{FcmNotification, FcmResponse}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object GoogleFcm {

  def sendWithPassThrough[T](conf: FcmSettings): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    FcmFlows.fcmWithData[T](conf)

  def send(conf: FcmSettings): Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf)

  def fireAndForget(conf: FcmSettings): Sink[FcmNotification, Future[Done]] =
    FcmFlows.fcm(conf).toMat(Sink.ignore)(Keep.right)

}
