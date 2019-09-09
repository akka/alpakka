/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import akka.stream.Materializer
import java.util.Base64

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub.impl.{GoogleSession, PubSubApi, TestCredentials}
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable.Seq
import java.time.Instant

class GooglePubSubSpec extends FlatSpec with MockitoSugar with ScalaFutures with Matchers {

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  private trait Fixtures {
    lazy val mockHttpApi = mock[PubSubApi]
    lazy val googlePubSub = new GooglePubSub {
      val httpApi = mockHttpApi
    }
    val http: HttpExt = mock[HttpExt]
    val config = PubSubConfig(TestCredentials.projectId, TestCredentials.clientEmail, TestCredentials.privateKey)
      .withSession(mock[GoogleSession])
  }

  it should "auth and publish the message" in new Fixtures {
    val flow = googlePubSub.publish(
      topic = "topic1",
      config = config
    )

    val request = PublishRequest(Seq(PublishMessage(data = base64String("Hello Google!"))))

    val source = Source(List(request))

    when(mockHttpApi.isEmulated).thenReturn(false)
    when(config.session.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.publish(project = TestCredentials.projectId,
                          topic = "topic1",
                          maybeAccessToken = Some("ok"),
                          request = request)
    ).thenReturn(Future.successful(Seq("id1")))

    val result = source.via(flow).runWith(Sink.seq)

    result.futureValue shouldBe Seq(Seq("id1"))
  }

  it should "publish the message without auth when emulated" in new Fixtures {

    override lazy val mockHttpApi = new PubSubApi {
      val PubSubGoogleApisHost: String = "..."
      val GoogleApisHost: String = "..."

      override def isEmulated: Boolean = true

      override def publish(
          project: String,
          topic: String,
          maybeAccessToken: Option[String],
          request: PublishRequest
      )(implicit as: ActorSystem, materializer: Materializer): Future[Seq[String]] =
        Future.successful(Seq("id2"))
    }

    val flow = googlePubSub.publish(
      topic = "topic2",
      config = config
    )

    val request = PublishRequest(Seq(PublishMessage(data = base64String("Hello Google!"))))

    val source = Source(List(request))
    val result = source.via(flow).runWith(Sink.seq)
    result.futureValue shouldBe Seq(Seq("id2"))
  }

  it should "subscribe and pull a message" in new Fixtures {
    val publishTime = Instant.ofEpochMilli(111)
    val message =
      ReceivedMessage(
        ackId = "1",
        message = PubSubMessage(messageId = "1", data = Some(base64String("Hello Google!")), publishTime = publishTime)
      )

    when(config.session.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.pull(project = TestCredentials.projectId,
                       subscription = "sub1",
                       maybeAccessToken = Some("ok"),
                       returnImmediately = true,
                       maxMessages = 1000)
    ).thenReturn(Future.successful(PullResponse(receivedMessages = Some(Seq()))))
      .thenReturn(Future.successful(PullResponse(receivedMessages = Some(Seq(message)))))

    val source = googlePubSub.subscribe(
      subscription = "sub1",
      config = config
    )

    val result = source.take(1).runWith(Sink.seq)

    result.futureValue shouldBe Seq(message)
  }

  it should "auth and acknowledge a message" in new Fixtures {
    when(config.session.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.acknowledge(
        project = TestCredentials.projectId,
        subscription = "sub1",
        maybeAccessToken = Some("ok"),
        request = AcknowledgeRequest(ackIds = "a1")
      )
    ).thenReturn(Future.successful(()))

    val sink = googlePubSub.acknowledge(
      subscription = "sub1",
      config = config
    )

    val source = Source(List(AcknowledgeRequest("a1")))

    val result = source.runWith(sink)

    result.futureValue shouldBe Done
  }

  it should "acknowledge a message without auth when emulated" in new Fixtures {
    override lazy val mockHttpApi = new PubSubApi {
      val PubSubGoogleApisHost: String = "..."
      val GoogleApisHost: String = "..."

      override def isEmulated: Boolean = true
      override def acknowledge(
          project: String,
          subscription: String,
          maybeAccessToken: Option[String],
          request: AcknowledgeRequest
      )(implicit as: ActorSystem, materializer: Materializer): Future[Unit] =
        Future.successful(())
    }

    val sink = googlePubSub.acknowledge(
      subscription = "sub1",
      config = config
    )

    val source = Source(List(AcknowledgeRequest("a1")))

    val result = source.runWith(sink)

    result.futureValue shouldBe Done
  }

  def base64String(s: String) =
    new String(Base64.getEncoder.encode(s.getBytes))
}
