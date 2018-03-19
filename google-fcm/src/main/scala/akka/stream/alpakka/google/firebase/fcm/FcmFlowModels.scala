/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

import akka.NotUsed
import akka.http.scaladsl.HttpExt
import akka.stream.Materializer
import akka.stream.alpakka.google.firebase.fcm.impl.{FcmSender, GoogleSession, GoogleTokenApi}
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

object FcmFlowModels {

  case class FcmFlowConfig(clientEmail: String,
                           privateKey: String,
                           projectid: String,
                           isTest: Boolean = false,
                           maxConcurentConnections: Int = 100)

  sealed trait FcmResponse
  case class FcmSuccessResponse(name: String) extends FcmResponse
  case class FcmErrorResponse(rawError: String) extends FcmResponse
  case class FcmSend(validate_only: Boolean, message: FcmNotification)

  protected[fcm] def fcmWithData[T](conf: FcmFlowConfig, http: => HttpExt, sender: FcmSender)(
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

  protected[fcm] def fcm(conf: FcmFlowConfig, http: => HttpExt, sender: FcmSender)(
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
