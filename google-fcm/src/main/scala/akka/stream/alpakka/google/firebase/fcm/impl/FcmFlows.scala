/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.impl
import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.stream.alpakka.google.firebase.fcm._
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] object FcmFlows {

  private[fcm] def fcmWithData[T](conf: FcmSettings,
                                  sender: FcmSender): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    Setup
      .flow { implicit materializer => attributes =>
        import materializer.executionContext
        val http = Http()(materializer.system)
        val session: GoogleSession = new GoogleSession(conf.clientEmail, conf.privateKey, new GoogleTokenApi(http))
        Flow[(FcmNotification, T)]
          .mapAsync(conf.maxConcurentConnections)(
            in =>
              session.getToken().flatMap { token =>
                sender.send(conf.projectid, token, http, FcmSend(conf.isTest, in._1)).zip(Future.successful(in._2))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  private[fcm] def fcm(conf: FcmSettings, sender: FcmSender): Flow[FcmNotification, FcmResponse, NotUsed] =
    Setup
      .flow { implicit materializer => attributes =>
        import materializer.executionContext
        val http = Http()(materializer.system)
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
      .mapMaterializedValue(_ => NotUsed)
}
