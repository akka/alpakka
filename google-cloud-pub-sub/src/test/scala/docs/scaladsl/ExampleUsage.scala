/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.time.Instant
import java.util.Base64

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.RestartSettings
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
  val config = PubSubConfig()
  val topic = "topic1"
  val subscription = "subscription1"
  //#init-system

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
  val resultPromise = Promise[Seq[String]]()

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

  //#publish with ordering key
  val messageWithOrderingKey =
    PublishMessage(new String(Base64.getEncoder.encode("Hello Google!".getBytes)), None, Some("my-ordering-key"))
  val publishedMessageWithOrderingKeyIds: Future[Seq[Seq[String]]] = Source
    .single(PublishRequest(Seq(messageWithOrderingKey)))
    .via(publishFlow)
    .runWith(Sink.seq)
  //#publish with ordering key

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
      RestartFlow.withBackoff(RestartSettings(1.second, 30.seconds, randomFactor = 0.2))(
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
