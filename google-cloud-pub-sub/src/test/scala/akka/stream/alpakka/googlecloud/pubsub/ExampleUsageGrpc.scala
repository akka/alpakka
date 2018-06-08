/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSubGrpc
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import com.google.protobuf.ByteString
import com.google.pubsub.v1
import com.google.pubsub.v1.PubsubMessage

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleUsageGrpc {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val projectId = "test-XXXXX"
  val topic = "topic1"
  val subscription = "subscription1"

  val client =
    GooglePubSubGrpc(
      project = projectId,
      subscription = subscription,
      PubSubConfig(
        host = "pubsub.googleapis.com",
        port = 443,
        usePlaintext = false,
        returnImmediately = true,
        maxMessages = 1000
      ),
      parallelism = 2,
      retryOnFailure = true,
      maxConsecutiveFailures = 5
    )

  //#init-client

  //#publish-single
  val publishMessage: PubsubMessage =
    v1.PubsubMessage
      .newBuilder()
      .setData(ByteString.copyFromUtf8("Hello world!"))
      .build()

  val publishRequest: v1.PublishRequest =
    v1.PublishRequest
      .newBuilder()
      .setTopic(topic)
      .addMessages(publishMessage)
      .build()

  val source: Source[v1.PublishRequest, NotUsed] =
    Source.single(publishRequest)

  val publishFlow: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed] =
    client.publish

  val publishedMessageIds: Future[Seq[v1.PublishResponse]] = source.via(publishFlow).runWith(Sink.seq)
  //#publish-single

  //#publish-fast
  val messageSource: Source[PubsubMessage, NotUsed] = Source(List(publishMessage, publishMessage))
  messageSource
    .groupedWithin(1000, 1.minute)
    .map { msgs =>
      v1.PublishRequest
        .newBuilder()
        .setTopic(topic)
        .addAllMessages(msgs.asJava)
        .build()
    }
    .via(publishFlow)
    .to(Sink.seq)
  //#publish-fast

  //#subscribe
  val subscriptionSource: Source[v1.ReceivedMessage, NotUsed] =
    client.subscribe

  val ackSink: Sink[AcknowledgeRequest, Future[Done]] =
    client.acknowledge

  subscriptionSource
    .map { message =>
      // do something fun
      message.getAckId
    }
    .groupedWithin(1000, 1.minute)
    .map(AcknowledgeRequest.apply)
    .to(ackSink)
  //#subscribe

  //#subscribe-auto-ack
  val subscribeMessageSource: Source[ReceivedMessage, NotUsed] = ???
  val processMessage: Sink[ReceivedMessage, NotUsed] = ???

  val batchAckSink: Sink[ReceivedMessage, NotUsed] =
    Flow[ReceivedMessage].map(_.ackId).groupedWithin(1000, 1.minute).map(AcknowledgeRequest.apply).to(ackSink)

  val q: RunnableGraph[NotUsed] = subscribeMessageSource.alsoTo(batchAckSink).to(processMessage)
  //#subscribe-auto-ack

}
