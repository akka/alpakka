/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import java.security.PrivateKey
import java.util.Base64

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.GooglePubSub
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._

import scala.concurrent.Future
import scala.concurrent.duration._

import scala.collection.immutable.Seq

class GooglePubSubSpec extends FlatSpec with MockitoSugar with ScalaFutures with Matchers {

  implicit val defaultPatience =
    PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  private trait Fixtures {
    val mockHttpApi = mock[HttpApi]
    val auth = mock[Session]
    val googlePubSub = new GooglePubSub {
      val httpApi = mockHttpApi

      def getSession(clientEmail: String, privateKey: PrivateKey) = auth
    }
  }

  it should "auth and publish the message" in new Fixtures {
    val flow = googlePubSub.publish(
      projectId = TestCredentials.projectId,
      apiKey = TestCredentials.apiKey,
      clientEmail = TestCredentials.clientEmail,
      privateKey = TestCredentials.privateKey,
      topic = "topic1"
    )

    val request = PublishRequest(Seq(PubSubMessage(messageId = "1", data = base64String("Hello Google!"))))

    val source = Source(List(request))

    when(auth.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.publish(project = TestCredentials.projectId,
                          topic = "topic1",
                          accessToken = "ok",
                          apiKey = TestCredentials.apiKey,
                          request = request)
    ).thenReturn(Future.successful(Seq("id1")))

    val result = source.via(flow).runWith(Sink.seq)

    result.futureValue shouldBe Seq(Seq("id1"))
  }

  it should "subscribe and pull a message" in new Fixtures {
    val message =
      ReceivedMessage(ackId = "1", message = PubSubMessage(messageId = "1", data = base64String("Hello Google!")))

    when(auth.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.pull(project = TestCredentials.projectId,
                       subscription = "sub1",
                       apiKey = TestCredentials.apiKey,
                       accessToken = "ok")
    ).thenReturn(Future.successful(PullResponse(receivedMessages = Some(Seq(message)))))

    val source = googlePubSub.subscribe(
      projectId = TestCredentials.projectId,
      apiKey = TestCredentials.apiKey,
      clientEmail = TestCredentials.clientEmail,
      privateKey = TestCredentials.privateKey,
      subscription = "sub1"
    )

    val result = source.take(1).runWith(Sink.seq)

    result.futureValue shouldBe Seq(message)
  }

  it should "auth and acknowledge a message" in new Fixtures {
    when(auth.getToken()).thenReturn(Future.successful("ok"))
    when(
      mockHttpApi.acknowledge(project = TestCredentials.projectId,
                              subscription = "sub1",
                              apiKey = TestCredentials.apiKey,
                              accessToken = "ok",
                              request = AcknowledgeRequest(ackIds = Seq("a1")))
    ).thenReturn(Future.successful(()))

    val sink = googlePubSub.acknowledge(
      projectId = TestCredentials.projectId,
      apiKey = TestCredentials.apiKey,
      clientEmail = TestCredentials.clientEmail,
      privateKey = TestCredentials.privateKey,
      subscription = "sub1"
    )

    val source = Source(List(AcknowledgeRequest(List("a1"))))

    val result = source.runWith(sink)

    result.futureValue shouldBe Done
  }

  def base64String(s: String) =
    new String(Base64.getEncoder.encode(s.getBytes))
}
