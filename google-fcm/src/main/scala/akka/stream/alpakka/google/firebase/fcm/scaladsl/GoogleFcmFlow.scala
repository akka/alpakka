/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.{FcmFlowConfig, FcmResponse}
import akka.stream.alpakka.google.firebase.fcm.impl.FcmSender
import akka.stream.alpakka.google.firebase.fcm.{FcmFlowModels, FcmNotification}
import akka.stream.scaladsl.Flow

object GoogleFcmFlow {

  def sendWithPassThrough[T](
      conf: FcmFlowConfig
  )(implicit materializer: Materializer,
    actorSystem: ActorSystem): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    FcmFlowModels.fcmWithData[T](conf, Http(), new FcmSender)

  def send(conf: FcmFlowConfig)(implicit materializer: Materializer,
                                actorSystem: ActorSystem): Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlowModels.fcm(conf, Http(), new FcmSender)

}
