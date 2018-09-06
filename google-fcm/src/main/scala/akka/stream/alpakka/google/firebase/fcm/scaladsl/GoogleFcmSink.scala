/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.FcmFlowConfig
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.alpakka.google.firebase.fcm.impl.{FcmFlows, FcmSender}
import akka.stream.scaladsl.Sink

object GoogleFcmSink {

  def fireAndForget(conf: FcmFlowConfig)(implicit materializer: Materializer,
                                         actorSystem: ActorSystem): Sink[FcmNotification, NotUsed] =
    FcmFlows.fcm(conf, Http(), new FcmSender).to(Sink.ignore)

}
