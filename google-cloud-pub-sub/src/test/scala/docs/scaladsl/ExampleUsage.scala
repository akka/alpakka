/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleUsage {

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
  val apiKey = "AIzaSyCVvqrlz057gCssc70n5JERyTW4TpB4ebE"

  val config = PubSubConfig(projectId, clientEmail, privateKey)

  val topic = "topic1"
  val subscription = "subscription1"
  //#init-credentials

  //#publish-single
  val publishMessage =
    PubSubMessage(new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
  val publishRequest = PublishRequest(Seq(publishMessage))

  val source: Source[PublishRequest, NotUsed] = Source.single(publishRequest)

  val publishFlow: Flow[PublishRequest, Seq[String], NotUsed] =
    GooglePubSub.publish(topic, config)

  val publishedMessageIds: Future[Seq[Seq[String]]] = source.via(publishFlow).runWith(Sink.seq)
  //#publish-single

  //#publish-fast
  val messageSource: Source[PubSubMessage, NotUsed] = Source(List(publishMessage, publishMessage))
  messageSource.groupedWithin(1000, 1.minute).map(PublishRequest.apply).via(publishFlow).to(Sink.seq)
  //#publish-fast

  //#subscribe
  val subscriptionSource: Source[ReceivedMessage, NotUsed] =
    GooglePubSub.subscribe(subscription, config)

  val ackSink: Sink[AcknowledgeRequest, Future[Done]] =
    GooglePubSub.acknowledge(subscription, config)

  subscriptionSource
    .map { message =>
      // do something fun

      message.ackId
    }
    .groupedWithin(1000, 1.minute)
    .map(AcknowledgeRequest.apply)
    .to(ackSink)
  //#subscribe

  //#subscribe-auto-ack
  val subscribeMessageSoruce: Source[ReceivedMessage, NotUsed] = ???
  val processMessage: Sink[ReceivedMessage, NotUsed] = ???

  val batchAckSink =
    Flow[ReceivedMessage].map(_.ackId).groupedWithin(1000, 1.minute).map(AcknowledgeRequest.apply).to(ackSink)

  val q = subscribeMessageSoruce.alsoTo(batchAckSink).to(processMessage)
  //#subscribe-auto-ack

}
