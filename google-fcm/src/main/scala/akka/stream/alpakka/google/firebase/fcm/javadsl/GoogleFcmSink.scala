/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.FcmFlowConfig
import akka.stream.alpakka.google.firebase.fcm.{FcmFlowModels, FcmNotification}
import akka.stream.alpakka.google.firebase.fcm.impl.FcmSender
import akka.stream.{javadsl, Materializer}
import akka.stream.scaladsl.Sink

object GoogleFcmSink {

  def fireAndForget(conf: FcmFlowConfig,
                    actorSystem: ActorSystem,
                    materializer: Materializer): javadsl.Sink[FcmNotification, NotUsed] =
    FcmFlowModels.fcm(conf, Http()(actorSystem), new FcmSender)(materializer).to(Sink.ignore).asJava

}
