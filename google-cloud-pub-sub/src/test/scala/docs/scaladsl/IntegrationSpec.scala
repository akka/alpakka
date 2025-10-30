/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import java.time.Instant
import java.util.Base64
import java.util.concurrent.TimeoutException

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class IntegrationSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Eventually
    with OptionValues
    with LogCapturing {

  private implicit val system: ActorSystem = ActorSystem("IntegrationSpec")

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  // The gCloud emulator is selected via environment parameters (in build.sbt)
  // as created in docker-compose.yml

  // as created in docker-compose.yml
  private val topic1 = "simpleTopic"
  private val topic1subscription = "simpleSubscription"

  // as created in docker-compose.yml
  private val topic2 = "testTopic"
  private val topic2subscription = "testSubscription"

  private val config = PubSubConfig()

  private def readable(msg: ReceivedMessage) = new String(Base64.getDecoder.decode(msg.message.data.get))

  "pub/sub" should {
    val SampleText = s"Hello Google! ${Instant.now.toString}"
    val SampleMessage = new String(Base64.getEncoder.encode(SampleText.getBytes))

    "publish a message and receive it again" in {
      // acknowledge any messages left on the subscription from earlier runs
      val cleanup = GooglePubSub
        .subscribe(topic1subscription, config)
        .idleTimeout(4.seconds)
        .map { msg =>
          println(readable(msg))
          msg.ackId
        }
        .map(id => AcknowledgeRequest(id))
        .via(GooglePubSub.acknowledgeFlow(topic1subscription, config))
        .runWith(Sink.ignore)

      cleanup.failed.futureValue shouldBe a[TimeoutException]

      // publish one new message
      val publishedMessageIds: Future[Seq[String]] =
        Source
          .single(PublishRequest(Seq(PublishMessage(SampleMessage))))
          .via(GooglePubSub.publish(topic1, config))
          .runWith(Sink.head)

      publishedMessageIds.futureValue.size shouldBe 1

      // expect the current message
      val sink = GooglePubSub
        .subscribe(topic1subscription, config)
        .take(1)
        .runWith(Sink.head)

      val received = sink.futureValue
      readable(received) shouldBe SampleText
    }

    "receive a published message and acknowledge it" in {
      val result = GooglePubSub
        .subscribe(topic2subscription, config)
        .map { message =>
          readable(message) shouldBe SampleText
          message.ackId
        }
        .groupedWithin(1, 1.second)
        .map(ids => AcknowledgeRequest(ids: _*))
        .via(GooglePubSub.acknowledgeFlow(topic2subscription, config))
        .runWith(Sink.headOption)

      val publishedMessageIds: Future[Seq[String]] =
        Source
          .single(PublishRequest(Seq(PublishMessage(SampleMessage))))
          .via(GooglePubSub.publish(topic2, config))
          .runWith(Sink.head)

      publishedMessageIds.futureValue.size shouldBe 1
      result.futureValue.value shouldBe Done

      // the acknowledged message should not arrive again
      val (stream, result2) = GooglePubSub
        .subscribe(topic2subscription, config)
        .toMat(TestSink())(Keep.both)
        .run()

      result2.ensureSubscription()
      result2.expectNoMessage(2.seconds)

      result2.cancel()
      stream.cancel()
    }
  }
}
