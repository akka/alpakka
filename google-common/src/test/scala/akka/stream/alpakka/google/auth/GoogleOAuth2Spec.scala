/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.stream.alpakka.google.{GoogleSettings, HoverflySupport}
import akka.testkit.TestKit
import io.specto.hoverfly.junit.core.SimulationSource.dsl
import io.specto.hoverfly.junit.core.model.RequestFieldMatcher.newRegexMatcher
import io.specto.hoverfly.junit.dsl.HoverflyDsl.service
import io.specto.hoverfly.junit.dsl.ResponseCreators.success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Clock
import scala.concurrent.ExecutionContext
import scala.io.Source

class GoogleOAuth2Spec
    extends TestKit(ActorSystem())
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with HoverflySupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
  implicit val defaultPatience = PatienceConfig(remainingOrDefault)

  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val settings = GoogleSettings(system)
  implicit val clock = Clock.systemUTC()

  lazy val privateKey = {
    val inputStream = getClass.getClassLoader.getResourceAsStream("private_pcks8.pem")
    Source.fromInputStream(inputStream).getLines().mkString("\n").stripMargin
  }

  lazy val publicKey = {
    val inputStream = getClass.getClassLoader.getResourceAsStream("key.pub")
    Source.fromInputStream(inputStream).getLines().mkString("\n").stripMargin
  }

  val scopes = Seq("https://www.googleapis.com/auth/service")

  "GoogleTokenApi" should {

    "call the api as the docs want to and return the token" in {
      hoverfly.simulate(
        dsl(
          service("oauth2.googleapis.com")
            .post("/token")
            .queryParam("prettyPrint", "false")
            .body(newRegexMatcher("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=*"))
            .willReturn(
              success("""{"access_token": "token", "token_type": "String", "expires_in": 3600}""", "application/json")
            )
        )
      )

      implicit val settings = GoogleSettings().requestSettings
      GoogleOAuth2.getAccessToken("email", privateKey, scopes).futureValue should matchPattern {
        case AccessToken("token", exp) if exp > (System.currentTimeMillis / 1000L + 3000L) =>
      }
    }
  }
}
