/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.japi.Pair
import akka.stream.alpakka.google.firebase.fcm.{FcmFlowConfig, FcmResponse}
import akka.stream.alpakka.google.firebase.fcm.impl.{FcmFlows, FcmSender}
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.scaladsl.Flow
import akka.stream.{javadsl, Materializer}

object GoogleFcmFlow {
  def sendWithPassThrough[T](
      conf: FcmFlowConfig,
      actorSystem: ActorSystem,
      materializer: Materializer
  ): javadsl.Flow[Pair[FcmNotification, T], Pair[FcmResponse, T], NotUsed] =
    Flow[Pair[FcmNotification, T]]
      .map(_.toScala)
      .via(FcmFlows.fcmWithData[T](conf, Http()(actorSystem), new FcmSender)(materializer))
      .map(t => Pair(t._1, t._2))
      .asJava

  def send(conf: FcmFlowConfig,
           actorSystem: ActorSystem,
           materializer: Materializer): javadsl.Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf, Http()(actorSystem), new FcmSender)(materializer).asJava
}
