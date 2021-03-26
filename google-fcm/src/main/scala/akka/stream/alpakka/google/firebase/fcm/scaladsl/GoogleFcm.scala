/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.stream.alpakka.google.firebase.fcm.impl.FcmFlows
import akka.stream.alpakka.google.firebase.fcm.{FcmNotification, FcmResponse, FcmSettings}
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
