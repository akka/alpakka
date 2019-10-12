/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.time.Instant
import java.util.Base64

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.googlecloud.pubsub.impl.{GoogleSession, PubSubApi, TestCredentials}
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class GooglePubSubSpec extends FlatSpec with MockitoSugar with ScalaFutures with Matchers {

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  private trait Fixtures {
    lazy val mockHttpApi = mock[PubSubApi]
    lazy val googlePubSub = new GooglePubSub {
      override val httpApi = mockHttpApi
    }
    def tokenFlow[T]: Flow[T, (T, Option[String], Unit), NotUsed] =
      Flow[T].map(request => (request, Some("ok"), ()))

    val http: HttpExt = mock[HttpExt]
    val config = PubSubConfig(TestCredentials.projectId, TestCredentials.clientEmail, TestCredentials.privateKey)
      .withSession(mock[GoogleSession])
  }

  it should "auth and publish the message" in new Fixtures {

    val request = PublishRequest(Seq(PublishMessage(data = base64String("Hello Google!"))))

    val source = Source(List(request))

    when(mockHttpApi.isEmulated).thenReturn(false)
    when(mockHttpApi.accessToken(config = config, parallelism = 1)).thenReturn(tokenFlow)
    when(
      mockHttpApi.publish[Unit](project = TestCredentials.projectId, topic = "topic1", parallelism = 1)
    ).thenReturn(Flow[(PublishRequest, Option[String], Unit)].map(_ => (Future.successful(Seq("id1")), ())))

    val flow = googlePubSub.publish(
      topic = "topic1",
      config = config
    )
    val result = source.via(flow).runWith(Sink.seq)

    result.futureValue shouldBe Seq(Seq("id1"))
  }

  it should "publish the message without auth when emulated" in new Fixtures {

    override lazy val mockHttpApi = new PubSubApi {
      val PubSubGoogleApisHost: String = "..."
      val PubSubGoogleApisPort = 80

      override def isEmulated: Boolean = true

      override def publish[T](
          project: String,
          topic: String,
          parallelism: Int,
          hcc: Option[HttpsConnectionContext] = None
      )(implicit as: ActorSystem,
        materializer: Materializer): Flow[(PublishRequest, Option[String], T), (Future[Seq[String]], T), NotUsed] =
        Flow[(PublishRequest, Option[String], T)].map {
          case (_, _, context) => (Future.successful(Seq("id2")), context)
        }
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
    private def flow(messages: Seq[ReceivedMessage]) =
      Flow[(Done, Option[String], Unit)]
        .map(_ => (Future.successful(PullResponse(receivedMessages = Some(messages))), ()))

    when(mockHttpApi.accessToken(config = config, parallelism = 1)).thenReturn(tokenFlow)
    when(
      mockHttpApi.pull[Unit](project = TestCredentials.projectId,
                             subscription = "sub1",
                             returnImmediately = true,
                             maxMessages = 1000,
                             parallelism = 1)
    ).thenReturn(flow(Seq(message)))

    val source = googlePubSub.subscribe(
      subscription = "sub1",
      config = config
    )

    val result = source.take(1).runWith(Sink.seq)

    result.futureValue shouldBe Seq(message)
  }

  it should "auth and acknowledge a message" in new Fixtures {
    when(mockHttpApi.accessToken(config = config, parallelism = 1)).thenReturn(tokenFlow)
    when(
      mockHttpApi.acknowledge[Unit](
        project = TestCredentials.projectId,
        subscription = "sub1",
        parallelism = 1
      )
    ).thenReturn(Flow[(AcknowledgeRequest, Option[String], Unit)].map(_ => (Future.successful(()), ())))

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
      val PubSubGoogleApisPort = 80

      override def isEmulated: Boolean = true
      override def acknowledge[T](project: String,
                                  subscription: String,
                                  parallelism: Int,
                                  hcc: Option[HttpsConnectionContext] = None)(
          implicit as: ActorSystem,
          materializer: Materializer
      ): Flow[(AcknowledgeRequest, Option[String], T), (Future[Unit], T), NotUsed] =
        Flow[(AcknowledgeRequest, Option[String], T)].map { case (_, _, context) => (Future.successful(()), context) }
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
