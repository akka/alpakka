/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.stream.alpakka.google.firebase.fcm.FcmSettings
import akka.stream.alpakka.google.firebase.fcm.v1.models.{FcmNotification, FcmResponse}
import akka.stream.scaladsl.Flow
import akka.stream.alpakka.google.GoogleAttributes

import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[fcm] object FcmFlows {

  private[fcm] def fcmWithData[T](conf: FcmSettings): Flow[(FcmNotification, T), (FcmResponse, T), NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        val settings = GoogleAttributes.resolveSettings(mat, attr)
        val sender = new FcmSender()
        Flow[(FcmNotification, T)].mapAsync(conf.maxConcurrentConnections) { case (notification, data) =>
          sender
            .send(Http(mat.system), FcmSend(conf.isTest, notification))(mat, settings)
            .zip(Future.successful(data))
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private[fcm] def fcm(conf: FcmSettings): Flow[FcmNotification, FcmResponse, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        val settings = GoogleAttributes.resolveSettings(mat, attr)
        val sender = new FcmSender()
        Flow[FcmNotification].mapAsync(conf.maxConcurrentConnections) { notification =>
          sender.send(Http(mat.system), FcmSend(conf.isTest, notification))(mat, settings)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
}
