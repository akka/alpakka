/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
//#imports
import akka.stream.alpakka.google.firebase.fcm.FcmNotificationModels._
import akka.stream.alpakka.google.firebase.fcm.scaladsl.GoogleFcm
import akka.stream.alpakka.google.firebase.fcm._

//#imports
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}

import scala.collection.immutable
import scala.concurrent.Future

class FcmExamples {

  implicit val system = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

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
  val fcmConfig = FcmSettings(clientEmail, privateKey, projectId)
  //#init-credentials

  //#simple-send
  val notification = FcmNotification("Test", "This is a test notification!", Token("token"))
  Source
    .single(notification)
    .runWith(GoogleFcm.fireAndForget(fcmConfig))
  //#simple-send

  //#asFlow-send
  val result1: Future[immutable.Seq[FcmResponse]] =
    Source
      .single(notification)
      .via(GoogleFcm.send(fcmConfig))
      .map {
        case res @ FcmSuccessResponse(name) =>
          println(s"Successful $name")
          res
        case res @ FcmErrorResponse(errorMessage) =>
          println(s"Send error $errorMessage")
          res
      }
      .runWith(Sink.seq)
  //#asFlow-send

  //#withData-send
  val result2: Future[immutable.Seq[(FcmResponse, String)]] =
    Source
      .single((notification, "superData"))
      .via(GoogleFcm.sendWithPassThrough(fcmConfig))
      .runWith(Sink.seq)
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
