/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.huawei.pushkit.javadsl

import akka.stream.alpakka.huawei.pushkit._
import akka.stream.alpakka.huawei.pushkit.impl.PushKitFlows
import akka.stream.alpakka.huawei.pushkit.models.{PushKitNotification, Response}
import akka.stream.javadsl
import akka.{Done, NotUsed}

import java.util.concurrent.CompletionStage

object HmsPushKit {

  def send(conf: HmsSettings): javadsl.Flow[PushKitNotification, Response, NotUsed] =
    PushKitFlows.pushKit(conf).asJava

  def fireAndForget(conf: HmsSettings): javadsl.Sink[PushKitNotification, CompletionStage[Done]] =
    send(conf)
      .toMat(javadsl.Sink.ignore(), javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
