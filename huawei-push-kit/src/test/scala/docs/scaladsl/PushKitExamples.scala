/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
//#imports
import akka.stream.alpakka.huawei.pushkit._
import akka.stream.alpakka.huawei.pushkit.scaladsl.HmsPushKit
import akka.stream.alpakka.huawei.pushkit.models.AndroidConfig
import akka.stream.alpakka.huawei.pushkit.models.AndroidNotification
import akka.stream.alpakka.huawei.pushkit.models.BasicNotification
import akka.stream.alpakka.huawei.pushkit.models.ClickAction
import akka.stream.alpakka.huawei.pushkit.models.Condition
import akka.stream.alpakka.huawei.pushkit.models.ErrorResponse
import akka.stream.alpakka.huawei.pushkit.models.PushKitNotification
import akka.stream.alpakka.huawei.pushkit.models.PushKitResponse
import akka.stream.alpakka.huawei.pushkit.models.Response
import akka.stream.alpakka.huawei.pushkit.models.Tokens

//#imports
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink

import scala.collection.immutable
import scala.concurrent.Future

class PushKitExamples {

  implicit val system = ActorSystem()

  //#simple-send
  val config = HmsSettings()
  val notification: PushKitNotification =
    PushKitNotification.empty
      .withNotification(
        BasicNotification.empty
          .withTitle("title")
          .withBody("body")
      )
      .withAndroidConfig(
        AndroidConfig.empty
          .withNotification(
            AndroidNotification.empty
              .withClickAction(
                ClickAction.empty
                  .withType(3)
              )
          )
      )
      .withTarget(Tokens(Set[String]("token").toSeq))

  Source
    .single(notification)
    .runWith(HmsPushKit.fireAndForget(config))
  //#simple-send

  //#asFlow-send
  val result1: Future[immutable.Seq[Response]] =
    Source
      .single(notification)
      .via(HmsPushKit.send(config))
      .map {
        case res @ PushKitResponse(code, msg, requestId) =>
          println(s"Response $res")
          res
        case res @ ErrorResponse(errorMessage) =>
          println(s"Send error $res")
          res
      }
      .runWith(Sink.seq)
  //#asFlow-send

  //#condition-builder
  import akka.stream.alpakka.huawei.pushkit.models.Condition.{Topic => CTopic}
  val condition = Condition(CTopic("TopicA") && (CTopic("TopicB") || (CTopic("TopicC") && !CTopic("TopicD"))))
  //#condition-builder
}
