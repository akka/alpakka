/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.{ActorContext, ActorSystem, Props}
import akka.http.scaladsl.model.ErrorInfo
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.google.{GoogleSettings, RequestSettings}
import akka.stream.alpakka.google.auth.OAuth2Credentials.TokenRequest
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

  implicit val settings = GoogleSettings().requestSettings
  implicit val clock = Clock.systemUTC()

  object AccessTokenProvider {
    var accessTokenPromise: Promise[AccessToken] = Promise.failed(new RuntimeException)
  }

  val testableCredentials = system.actorOf {
    Props {
      new OAuth2CredentialsActor {
        override protected def getAccessToken()(implicit ctx: ActorContext,
                                                settings: RequestSettings): Future[AccessToken] = {
          AccessTokenProvider.accessTokenPromise.future
        }
      }
    }
  }

  "OAuth2Credentials" should {

    "queue requests until token arrives, then respond" in {

      AccessTokenProvider.accessTokenPromise = Promise()

      val request1 = Promise[OAuth2BearerToken]()
      val request2 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request1, settings)
      testableCredentials ! TokenRequest(request2, settings)

      assertThrows[TestFailedException](request1.future.futureValue)
      assertThrows[TestFailedException](request2.future.futureValue)

      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))

      request1.future.futureValue shouldEqual OAuth2BearerToken("first token")
      request2.future.futureValue shouldEqual OAuth2BearerToken("first token")
    }

    "fail requests if token request fails" in {

      AccessTokenProvider.accessTokenPromise = Promise()

      val request1 = Promise[OAuth2BearerToken]()
      val request2 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request1, settings)
      testableCredentials ! TokenRequest(request2, settings)

      AccessTokenProvider.accessTokenPromise.failure(GoogleOAuth2Exception(ErrorInfo()))

      assert(request1.future.failed.futureValue.isInstanceOf[GoogleOAuth2Exception])
      assert(request2.future.failed.futureValue.isInstanceOf[GoogleOAuth2Exception])
    }

    "refresh token (only) when expired" in {

      AccessTokenProvider.accessTokenPromise = Promise()
      val request1 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request1, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))
      request1.future.futureValue shouldEqual OAuth2BearerToken("first token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request2 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request2, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("second token", JwtTime.nowSeconds + 120))
      request2.future.futureValue shouldEqual OAuth2BearerToken("second token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request3 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request3, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("third token", JwtTime.nowSeconds + 1))
      request3.future.futureValue shouldEqual OAuth2BearerToken("second token")
    }

  }

}
