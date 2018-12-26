/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.scaladsl

//#imports
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.FcmFlowConfig
import akka.stream.alpakka.google.firebase.fcm.{FcmFlowModels, FcmNotification}
import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels._
import akka.stream.scaladsl.{Sink, Source}

import scala.collection.immutable
import scala.concurrent.Future
//#imports

class Examples {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-credentials
  val privateKey =
    """-----BEGIN RSA PRIVATE KEY-----
      |MIIBOgIBAAJBAJHPYfmEpShPxAGP12oyPg0CiL1zmd2V84K5dgzhR9TFpkAp2kl2
      |9BTc8jbAY0dQW4Zux+hyKxd6uANBKHOWacUCAwEAAQJAQVyXbMS7TGDFWnXieKZh
      |Dm/uYA6sEJqheB4u/wMVshjcQdHbi6Rr0kv7dCLbJz2v9bVmFu5i8aFnJy1MJOpA
      |2QIhAPyEAaVfDqJGjVfryZDCaxrsREmdKDlmIppFy78/d8DHAiEAk9JyTHcapckD
      |uSyaE6EaqKKfyRwSfUGO1VJXmPjPDRMCIF9N900SDnTiye/4FxBiwIfdynw6K3dW
      |fBLb6uVYr/r7AiBUu/p26IMm6y4uNGnxvJSqe+X6AxR6Jl043OWHs4AEbwIhANuz
      |Ay3MKOeoVbx0L+ruVRY5fkW+oLHbMGtQ9dZq7Dp9
      |-----END RSA PRIVATE KEY-----""".stripMargin
  val clientEmail = "test-XXX@test-XXXXX.iam.gserviceaccount.com"
  val projectId = "test-XXXXX"
  val fcmConfig = FcmFlowConfig(clientEmail, privateKey, projectId, isTest = false, maxConcurentConnections = 100)
  //#init-credentials

  //#simple-send
  val notification = FcmNotification("Test", "This is a test notification!", Token("token"))
  Source.single(notification).runWith(GoogleFcmSink.fireAndForget(fcmConfig))
  //#simple-send

  //#asFlow-send
  val result1: Future[immutable.Seq[FcmFlowModels.FcmResponse]] =
    Source.single(notification).via(GoogleFcmFlow.send(fcmConfig)).runWith(Sink.seq)
  //#asFlow-send

  //#withData-send
  val result2: Future[immutable.Seq[(FcmFlowModels.FcmResponse, String)]] =
    Source.single((notification, "superData")).via(GoogleFcmFlow.sendWithPassThrough(fcmConfig)).runWith(Sink.seq)
  //#withData-send

  //#noti-create
  val buildedNotification = FcmNotification.empty
    .withTarget(Topic("testers"))
    .withBasicNotification("title", "body")
    //.withAndroidConfig(AndroidConfig(...))
    //.withApnsConfig(ApnsConfig(...))
    .withWebPushConfig(
      WebPushConfig(
        headers = Map.empty,
        data = Map.empty,
        WebPushNotification("web-title", "web-body", "http://example.com/icon.png")
      )
    )
  val sendable = buildedNotification.isSendable
  //#noti-create

  //#condition-builder
  import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels.Condition.{Topic => CTopic}
  val condition = Condition(CTopic("TopicA") && (CTopic("TopicB") || (CTopic("TopicC") && !CTopic("TopicD"))))
  val conditioneddNotification = FcmNotification("Test", "This is a test notification!", condition)
  //#condition-builder

}
