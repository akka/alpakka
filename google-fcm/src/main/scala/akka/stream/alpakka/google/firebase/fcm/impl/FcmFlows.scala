/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl
import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.FcmNotification
import akka.stream.alpakka.google.firebase.fcm._
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] object FcmFlows {

  private[fcm] def fcmWithData[T](conf: FcmSettings, http: => HttpExt, sender: FcmSender)(
      implicit materializer: Materializer
  ): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] = {
    import materializer.executionContext
    val session: GoogleSession = new GoogleSession(conf.clientEmail, conf.privateKey, new GoogleTokenApi(http))
    Flow[(FcmNotification, T)]
      .mapAsync(conf.maxConcurentConnections)(
        in =>
          session.getToken().flatMap { token =>
            sender.send(conf.projectid, token, http, FcmSend(conf.isTest, in._1)).zip(Future.successful(in._2))
        }
      )
  }

  private[fcm] def fcm(conf: FcmSettings, http: => HttpExt, sender: FcmSender)(
      implicit materializer: Materializer
  ): Flow[FcmNotification, FcmResponse, NotUsed] = {
    import materializer.executionContext
    val session: GoogleSession = new GoogleSession(conf.clientEmail, conf.privateKey, new GoogleTokenApi(http))
    val sender: FcmSender = new FcmSender()
    Flow[FcmNotification]
      .mapAsync(conf.maxConcurentConnections)(
        in =>
          session.getToken().flatMap { token =>
            sender.send(conf.projectid, token, http, FcmSend(conf.isTest, in))
        }
      )
  }
}
