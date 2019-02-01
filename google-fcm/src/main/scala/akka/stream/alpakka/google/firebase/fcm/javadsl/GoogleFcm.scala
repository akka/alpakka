/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.japi.Pair
import akka.stream.alpakka.google.firebase.fcm.{FcmResponse, FcmSettings}
import akka.stream.alpakka.google.firebase.fcm.impl.{FcmFlows, FcmSender}
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.{javadsl, scaladsl, Materializer}

object GoogleFcm {

  def sendWithPassThrough[T](
      conf: FcmSettings,
      actorSystem: ActorSystem,
      materializer: Materializer
  ): javadsl.Flow[Pair[FcmNotification, T], Pair[FcmResponse, T], NotUsed] =
    scaladsl
      .Flow[Pair[FcmNotification, T]]
      .map(_.toScala)
      .via(FcmFlows.fcmWithData[T](conf, Http()(actorSystem), new FcmSender)(materializer))
      .map(t => Pair(t._1, t._2))
      .asJava

  def send(conf: FcmSettings,
           actorSystem: ActorSystem,
           materializer: Materializer): javadsl.Flow[FcmNotification, FcmResponse, NotUsed] =
    FcmFlows.fcm(conf, Http()(actorSystem), new FcmSender)(materializer).asJava

  def fireAndForget(conf: FcmSettings,
                    actorSystem: ActorSystem,
                    materializer: Materializer): javadsl.Sink[FcmNotification, CompletionStage[Done]] =
    send(conf, actorSystem, materializer)
      .toMat(javadsl.Sink.ignore(), javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
