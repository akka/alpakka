/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class SessionSpec extends FlatSpec with ScalaFutures with MockitoSugar with BeforeAndAfterAll with Matchers {

  private trait Fixtures {
    val mockHttpApi = mock[HttpApi]
  }

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val accessToken =
    "ya29.Elz4A2XkfGKJ4CoS5x_umUBHsvjGdeWQzu6gRRCnNXI0fuIyoDP_6aYktBQEOI4YAhLNgUl2OpxWQaN8Z3hd5YfFw1y4EGAtr2o28vSID-c8ul_xxHuudE7RmhH9sg"

  it should "ask for new session on first request" in new Fixtures {
    val session = new Session(TestCredentials.clientEmail, TestCredentials.privateKey) {
      override def now = Instant.ofEpochSecond(0)
      override val httpApi = mockHttpApi
    }

    when(mockHttpApi.getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(0)))
      .thenReturn(Future.successful(AccessTokenExpiry(accessToken, 3600)))

    session.getToken().futureValue shouldBe accessToken

    verify(mockHttpApi, times(1))
      .getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(0))
  }

  it should "uses the cached value for the second request" in new Fixtures {
    val session = new Session(TestCredentials.clientEmail, TestCredentials.privateKey) {
      override def now = Instant.ofEpochSecond(0)
      override val httpApi = mockHttpApi
      this.maybeAccessToken = Some(Future.successful(AccessTokenExpiry(accessToken, 3600)))
    }

    session.getToken().futureValue shouldBe accessToken

    verify(mockHttpApi, times(0))
      .getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(0))
  }

  it should "requests a new session if current is about to expire" in new Fixtures {
    val session = new Session(TestCredentials.clientEmail, TestCredentials.privateKey) {
      override def now = Instant.ofEpochSecond(3599)
      override val httpApi = mockHttpApi
      this.maybeAccessToken = Some(Future.successful(AccessTokenExpiry("t1", 3600)))
    }

    when(
      mockHttpApi.getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(3599))
    ).thenReturn(Future.successful(AccessTokenExpiry(accessToken, 3600)))

    session.getToken().futureValue shouldBe accessToken

    verify(mockHttpApi, times(1))
      .getAccessToken(TestCredentials.clientEmail, TestCredentials.privateKey, Instant.ofEpochSecond(3599))
  }

  override def afterAll(): Unit =
    Await.result(system.terminate(), 5.seconds)
}
