/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.{FcmResponse, FcmSettings}
import akka.stream.alpakka.google.firebase.fcm.impl.{FcmFlows, FcmSender}
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

object GoogleFcm {

  def sendWithPassThrough[T](
      conf: FcmSettings
  )(implicit materializer: Materializer,
    actorSystem: ActorSystem): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    FcmFlows.fcmWithData[T](conf, Http(), new FcmSender)

  def send(conf: FcmSettings)(implicit materializer: Materializer,
                              actorSystem: ActorSystem): Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf, Http(), new FcmSender)

  def fireAndForget(conf: FcmSettings)(implicit materializer: Materializer,
                                       actorSystem: ActorSystem): Sink[FcmNotification, Future[Done]] =
    FcmFlows.fcm(conf, Http(), new FcmSender).toMat(Sink.ignore)(Keep.right)

}
