/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl.{GrpcPublisher, PubSubAttributes}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.OptionValues

//#publish-single
import akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl.GooglePubSub
import akka.stream.scaladsl._
import com.google.pubsub.v1.pubsub._

//#publish-single

import akka.NotUsed
import com.google.protobuf.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inside}

import scala.concurrent.duration._
import scala.concurrent.Future
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IntegrationSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with BeforeAndAfterAll
    with ScalaFutures
    with OptionValues
    with LogCapturing {

  implicit val system = ActorSystem("IntegrationSpec")

  implicit val defaultPatience = PatienceConfig(timeout = 15.seconds, interval = 50.millis)

  "connector" should {

    "publish a message" in {
      //#publish-single
      val projectId = "alpakka"
      val topic = "simpleTopic"

      val publishMessage: PubsubMessage =
        PubsubMessage()
          .withData(ByteString.copyFromUtf8("Hello world!"))

      val publishRequest: PublishRequest =
        PublishRequest()
          .withTopic(s"projects/$projectId/topics/$topic")
          .addMessages(publishMessage)

      val source: Source[PublishRequest, NotUsed] =
        Source.single(publishRequest)

      val publishFlow: Flow[PublishRequest, PublishResponse, NotUsed] =
        GooglePubSub.publish(parallelism = 1)

      val publishedMessageIds: Future[Seq[PublishResponse]] = source.via(publishFlow).runWith(Sink.seq)
      //#publish-single

      publishedMessageIds.futureValue should not be empty
    }

    "publish batch" in {
      //#publish-fast
      val projectId = "alpakka"
      val topic = "simpleTopic"

      val publishMessage: PubsubMessage =
        PubsubMessage()
          .withData(ByteString.copyFromUtf8("Hello world!"))

      val messageSource: Source[PubsubMessage, NotUsed] = Source(List(publishMessage, publishMessage))
      val published = messageSource
        .groupedWithin(1000, 1.minute)
        .map { msgs =>
          PublishRequest()
            .withTopic(s"projects/$projectId/topics/$topic")
            .addAllMessages(msgs)
        }
        .via(GooglePubSub.publish(parallelism = 1))
        .runWith(Sink.seq)
      //#publish-fast

      published.futureValue should not be empty
    }

    "subscribe streaming" in {
      //#subscribe-stream
      val projectId = "alpakka"
      val subscription = "simpleSubscription"

      val request = StreamingPullRequest()
        .withSubscription(s"projects/$projectId/subscriptions/$subscription")
        .withStreamAckDeadlineSeconds(10)

      val subscriptionSource: Source[ReceivedMessage, Future[Cancellable]] =
        GooglePubSub.subscribe(request, pollInterval = 1.second)
      //#subscribe-stream

      val first = subscriptionSource.runWith(Sink.head)

      val topic = "simpleTopic"
      val msg = ByteString.copyFromUtf8("Hello world!")

      val publishMessage: PubsubMessage =
        PubsubMessage().withData(msg)

      val publishRequest: PublishRequest =
        PublishRequest()
          .withTopic(s"projects/$projectId/topics/$topic")
          .addMessages(publishMessage)

      Source.single(publishRequest).via(GooglePubSub.publish(parallelism = 1)).runWith(Sink.ignore)

      first.futureValue.message.value.data shouldBe msg
    }

    "subscribe sync" in {
      //#subscribe-sync
      val projectId = "alpakka"
      val subscription = "simpleSubscription"

      val request = PullRequest()
        .withSubscription(s"projects/$projectId/subscriptions/$subscription")
        .withMaxMessages(10)

      val subscriptionSource: Source[ReceivedMessage, Future[Cancellable]] =
        GooglePubSub.subscribePolling(request, pollInterval = 1.second)
      //#subscribe-sync

      val first = subscriptionSource.runWith(Sink.head)

      val topic = "simpleTopic"
      val msg = ByteString.copyFromUtf8("Hello world!")

      val publishMessage: PubsubMessage =
        PubsubMessage().withData(msg)

      val publishRequest: PublishRequest =
        PublishRequest()
          .withTopic(s"projects/$projectId/topics/$topic")
          .addMessages(publishMessage)

      Source.single(publishRequest).via(GooglePubSub.publish(parallelism = 1)).runWith(Sink.ignore)

      first.futureValue.message.value.data shouldBe msg
    }

    "acknowledge" in {
      val projectId = "alpakka"
      val subscription = "simpleSubscription"

      val request = StreamingPullRequest()
        .withSubscription(s"projects/$projectId/subscriptions/$subscription")
        .withStreamAckDeadlineSeconds(10)

      val subscriptionSource: Source[ReceivedMessage, Future[Cancellable]] =
        GooglePubSub.subscribe(request, pollInterval = 1.second)

      //#acknowledge
      val ackSink: Sink[AcknowledgeRequest, Future[Done]] =
        GooglePubSub.acknowledge(parallelism = 1)

      subscriptionSource
        .map { message =>
          // do something fun
          message.ackId
        }
        .groupedWithin(10, 1.second)
        .map(
          ids =>
            AcknowledgeRequest()
              .withSubscription(
                s"projects/$projectId/subscriptions/$subscription"
              )
              .withAckIds(ids)
        )
        .to(ackSink)
      //#acknowledge
    }

    "acknowledge flow" in {
      val projectId = "alpakka"
      val subscription = "simpleSubscription"

      val request = StreamingPullRequest()
        .withSubscription(s"projects/$projectId/subscriptions/$subscription")
        .withStreamAckDeadlineSeconds(10)

      val subscriptionSource: Source[ReceivedMessage, Future[Cancellable]] =
        GooglePubSub.subscribe(request, pollInterval = 1.second)

      subscriptionSource
        .map { message =>
          // do something fun
          message.ackId
        }
        .groupedWithin(10, 1.second)
        .map(ids => AcknowledgeRequest(ackIds = ids))
        .via(GooglePubSub.acknowledgeFlow())
        .to(Sink.ignore)
    }

    "republish" in {
      val msg = "Labas!"

      val projectId = "alpakka"
      val topic = "testTopic"
      val subscription = "testSubscription"

      val topicFqrs = s"projects/$projectId/topics/$topic"
      val subscriptionFqrs = s"projects/$projectId/subscriptions/$subscription"

      val pub = PublishRequest(topicFqrs, Seq(PubsubMessage(ByteString.copyFromUtf8(msg))))
      val pubResp = Source.single(pub).via(GooglePubSub.publish(parallelism = 1)).runWith(Sink.head)

      pubResp.futureValue.messageIds should not be empty

      val sub = StreamingPullRequest(subscriptionFqrs, streamAckDeadlineSeconds = 10)

      // subscribe but do not ack - message will be republished later
      val subNoAckResp = GooglePubSub.subscribe(sub, 1.second).runWith(Sink.head)

      inside(subNoAckResp.futureValue.message) {
        case Some(PubsubMessage(data, _, _, _, _, _)) => data.toStringUtf8 shouldBe msg
      }

      // subscribe and get the republished message, and ack this time
      val subWithAckResp = GooglePubSub
        .subscribe(sub, 1.second)
        .alsoTo(
          Flow[ReceivedMessage]
            .map(msg => AcknowledgeRequest(subscriptionFqrs, Seq(msg.ackId)))
            .to(GooglePubSub.acknowledge(parallelism = 1))
        )
        .runWith(Sink.head)

      inside(subWithAckResp.futureValue.message) {
        case Some(PubsubMessage(data, _, _, _, _, _)) => data.toStringUtf8 shouldBe msg
      }

      // check if the message is not republished again
      GooglePubSub
        .subscribe(sub, 1.second)
        .idleTimeout(12.seconds)
        .runWith(Sink.ignore)
        .failed
        .futureValue
    }

    "custom publisher" in {
      // #attributes
      val settings = PubSubSettings(system)
      val publisher = GrpcPublisher(settings)

      val publishFlow: Flow[PublishRequest, PublishResponse, NotUsed] =
        GooglePubSub
          .publish(parallelism = 1)
          .withAttributes(PubSubAttributes.publisher(publisher))
      // #attributes

      Source.single(PublishRequest()).via(publishFlow).to(Sink.ignore)
    }
  }

  override def afterAll() =
    system.terminate()

}
