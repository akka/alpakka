/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.stream.alpakka.huawei.pushkit.HmsSettings
import akka.stream.alpakka.huawei.pushkit.models.{PushKitNotification, Response}
import akka.stream.scaladsl.Flow

/**
 * INTERNAL API
 */
@InternalApi
private[pushkit] object PushKitFlows {

  private[pushkit] def pushKit(conf: HmsSettings): Flow[PushKitNotification, Response, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        import materializer.executionContext
        val http = Http()(materializer.system)
        val session: HmsSession =
          new HmsSession(conf, new HmsTokenApi(http, materializer.system, conf.forwardProxy))
        val sender: PushKitSender = new PushKitSender()
        Flow[PushKitNotification]
          .mapAsync(conf.maxConcurrentConnections)(
            in =>
              session.getToken()(materializer).flatMap { token =>
                sender.send(conf, token, http, PushKitSend(conf.test, in), materializer.system)(materializer)
              }
          )
      }
      .mapMaterializedValue(_ => NotUsed)
}
