/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.FcmFlowConfig
import akka.stream.alpakka.google.firebase.fcm.{FcmFlowModels, FcmNotification}
import akka.stream.alpakka.google.firebase.fcm.impl.FcmSender
import akka.stream.scaladsl.Sink

object GoogleFcmSink {

  def fireAndForget(conf: FcmFlowConfig)(implicit materializer: Materializer,
                                         actorSystem: ActorSystem): Sink[FcmNotification, NotUsed] =
    FcmFlowModels.fcm(conf, Http(), new FcmSender).to(Sink.ignore)

}
