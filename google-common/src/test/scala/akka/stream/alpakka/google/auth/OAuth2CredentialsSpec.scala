/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ErrorInfo
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import akka.stream.alpakka.google.{GoogleSettings, RequestSettings}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import pdi.jwt.JwtTime

import java.time.Clock
import scala.concurrent.{Future, Promise}

class OAuth2CredentialsSpec
    extends TestKit(ActorSystem("OAuth2CredentialsSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  import system.dispatcher
  implicit val settings: RequestSettings = GoogleSettings().requestSettings
  implicit val clock: Clock = Clock.systemUTC()

  final object AccessTokenProvider {
    @volatile var accessTokenPromise: Promise[AccessToken] = Promise.failed(new RuntimeException)
  }

  val testableCredentials = new OAuth2Credentials("dummyProject") {
    override protected def getAccessToken()(implicit mat: Materializer,
                                            settings: RequestSettings,
                                            clock: Clock): Future[AccessToken] =
      AccessTokenProvider.accessTokenPromise.future
  }

  "OAuth2Credentials" should {

    "queue requests until token arrives, then respond" in {

      AccessTokenProvider.accessTokenPromise = Promise()

      val request1 = testableCredentials.get()
      val request2 = testableCredentials.get()

      assertThrows[TestFailedException](request1.futureValue)
      assertThrows[TestFailedException](request2.futureValue)

      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))

      request1.futureValue shouldEqual OAuth2BearerToken("first token")
      request2.futureValue shouldEqual OAuth2BearerToken("first token")
    }

    "fail requests if token request fails" in {

      AccessTokenProvider.accessTokenPromise = Promise()

      val request1 = testableCredentials.get()
      val request2 = testableCredentials.get()

      AccessTokenProvider.accessTokenPromise.failure(GoogleOAuth2Exception(ErrorInfo()))

      assert(request1.failed.futureValue.isInstanceOf[GoogleOAuth2Exception])
      assert(request2.failed.futureValue.isInstanceOf[GoogleOAuth2Exception])
    }

    "refresh token (only) when expired" in {

      AccessTokenProvider.accessTokenPromise = Promise()
      val request1 = testableCredentials.get()
      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))
      request1.futureValue shouldEqual OAuth2BearerToken("first token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request2 = testableCredentials.get()
      AccessTokenProvider.accessTokenPromise.success(AccessToken("second token", JwtTime.nowSeconds + 120))
      request2.futureValue shouldEqual OAuth2BearerToken("second token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request3 = testableCredentials.get()
      AccessTokenProvider.accessTokenPromise.success(AccessToken("third token", JwtTime.nowSeconds + 1))
      request3.futureValue shouldEqual OAuth2BearerToken("second token")
    }

  }

}
