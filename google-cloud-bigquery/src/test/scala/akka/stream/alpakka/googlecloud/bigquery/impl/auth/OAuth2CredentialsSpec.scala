/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.auth

import akka.actor.{ActorContext, ActorSystem, Props}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings
import akka.stream.alpakka.googlecloud.bigquery.impl.auth.OAuth2Credentials.TokenRequest
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import pdi.jwt.JwtTime

import java.time.Clock
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Try}

class OAuth2CredentialsSpec
    extends TestKit(ActorSystem("OAuth2CredentialsSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  implicit val settings = BigQuerySettings()
  implicit val clock = Clock.systemUTC()

  object AccessTokenProvider {
    var accessTokenPromise: Promise[AccessToken] = Promise.failed(new RuntimeException)
  }

  val testableCredentials = system.actorOf {
    Props {
      new OAuth2Credentials {
        override protected def getAccessToken()(implicit ctx: ActorContext,
                                                settings: BigQuerySettings): Future[AccessToken] = {
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

      Try(Await.result(request1.future, 100.milliseconds)) should matchPattern { case Failure(_) => }
      Try(Await.result(request2.future, 100.milliseconds)) should matchPattern { case Failure(_) => }

      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))

      Await.result(request1.future, 100.milliseconds) shouldEqual OAuth2BearerToken("first token")
      Await.result(request2.future, 100.milliseconds) shouldEqual OAuth2BearerToken("first token")
    }

    "fail requests if token request fails" in {

      AccessTokenProvider.accessTokenPromise = Promise()

      val request1 = Promise[OAuth2BearerToken]()
      val request2 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request1, settings)
      testableCredentials ! TokenRequest(request2, settings)

      AccessTokenProvider.accessTokenPromise.failure(new RuntimeException)

      Try(Await.result(request1.future, 100.milliseconds)) should matchPattern { case Failure(_) => }
      Try(Await.result(request2.future, 100.milliseconds)) should matchPattern { case Failure(_) => }
    }

    "refresh token (only) when expired" in {

      AccessTokenProvider.accessTokenPromise = Promise()
      val request1 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request1, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("first token", JwtTime.nowSeconds + 1))
      Await.result(request1.future, 100.seconds) shouldEqual OAuth2BearerToken("first token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request2 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request2, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("second token", JwtTime.nowSeconds + 120))
      Await.result(request2.future, 100.seconds) shouldEqual OAuth2BearerToken("second token")

      AccessTokenProvider.accessTokenPromise = Promise()
      val request3 = Promise[OAuth2BearerToken]()
      testableCredentials ! TokenRequest(request3, settings)
      AccessTokenProvider.accessTokenPromise.success(AccessToken("third token", JwtTime.nowSeconds + 1))
      Await.result(request3.future, 100.seconds) shouldEqual OAuth2BearerToken("second token")
    }

  }

}
