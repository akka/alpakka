/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.time.Instant
import java.util.Base64

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Flow, FlowWithContext, RestartFlow, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class ExampleUsage {

  //#init-system
  implicit val system = ActorSystem()
  //#init-system

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
    PublishMessage(new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
  val publishRequest = PublishRequest(Seq(publishMessage))

  val source: Source[PublishRequest, NotUsed] = Source.single(publishRequest)

  val publishFlow: Flow[PublishRequest, Seq[String], NotUsed] =
    GooglePubSub.publish(topic, config)

  val publishedMessageIds: Future[Seq[Seq[String]]] = source.via(publishFlow).runWith(Sink.seq)
  //#publish-single

  //#publish-single-with-context
  val publishMessageWithContext =
    PublishMessage(new String(Base64.getEncoder.encode("Hello Google!".getBytes)))
  val publishRequestWithContext = PublishRequest(Seq(publishMessage))
  val resultPromise = Promise[Seq[String]]

  val sourceWithContext: Source[(PublishRequest, Promise[Seq[String]]), NotUsed] =
    Source.single(publishRequest -> resultPromise)

  val publishFlowWithContext
      : FlowWithContext[PublishRequest, Promise[Seq[String]], Seq[String], Promise[Seq[String]], NotUsed] =
    GooglePubSub.publishWithContext[Promise[Seq[String]]](topic, config)

  val publishedMessageIdsWithContext: Future[Seq[(Seq[String], Promise[Seq[String]])]] =
    sourceWithContext.via(publishFlowWithContext).runWith(Sink.seq)
  //#publish-single-with-context

  //#publish-fast
  val messageSource: Source[PublishMessage, NotUsed] = Source(List(publishMessage, publishMessage))
  messageSource
    .groupedWithin(1000, 1.minute)
    .map(grouped => PublishRequest(grouped))
    .via(publishFlow)
    .to(Sink.seq)
  //#publish-fast

  //#subscribe
  val subscriptionSource: Source[ReceivedMessage, Cancellable] =
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

  //#subscribe-source-control
  Source
    .tick(0.seconds, 10.seconds, Done)
    .via(
      RestartFlow.withBackoff(1.second, 30.seconds, randomFactor = 0.2)(
        () => GooglePubSub.subscribeFlow(subscription, config)
      )
    )
    .map { message =>
      // do something fun

      message.ackId
    }
    .groupedWithin(1000, 1.minute)
    .map(AcknowledgeRequest.apply)
    .to(ackSink)
  //#subscribe-source-control

  //#subscribe-auto-ack
  val subscribeMessageSoruce: Source[ReceivedMessage, NotUsed] = // ???
    //#subscribe-auto-ack
    Source.single(ReceivedMessage("id", PubSubMessage(Some("data"), None, "msg-id-1", Instant.now)))
  //#subscribe-auto-ack
  val processMessage: Sink[ReceivedMessage, NotUsed] = // ???
    //#subscribe-auto-ack
    Flow[ReceivedMessage].to(Sink.ignore)
  //#subscribe-auto-ack

  val batchAckSink =
    Flow[ReceivedMessage].map(_.ackId).groupedWithin(1000, 1.minute).map(AcknowledgeRequest.apply).to(ackSink)

  val q = subscribeMessageSoruce.alsoTo(batchAckSink).to(processMessage)
  //#subscribe-auto-ack

}
