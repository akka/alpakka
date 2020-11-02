/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl.auth

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{`Raw-Request-URI`, Host, RawHeader}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region

import scala.compat.java8.OptionConverters._

class SignerSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LogCapturing {
  def this() = this(ActorSystem("SignerSpec"))

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  val credentials = StaticCredentialsProvider.create(
    AwsBasicCredentials.create("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
  )

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  def signingKey(dateTime: ZonedDateTime) =
    SigningKey(dateTime, credentials, CredentialScope(dateTime.toLocalDate, Region.US_EAST_1, "iam"))

  val cr = CanonicalRequest(
    "GET",
    "/",
    "Action=ListUsers&Version=2010-05-08",
    "content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\nx-amz-date:20150830T123600Z",
    "content-type;host;x-amz-date",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  )

  "Signer" should "calculate the string to sign" in {
    val date = LocalDateTime.of(2015, 8, 30, 12, 36, 0).atZone(ZoneOffset.UTC)
    val stringToSign: String = Signer.stringToSign("AWS4-HMAC-SHA256", signingKey(date), date, cr)
    stringToSign should equal(
      "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nf536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
    )
  }

  it should "add the date, content hash, and authorization headers to a request" in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")
      .withHeaders(
        Host("iam.amazonaws.com"),
        RawHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
      )

    val date = LocalDateTime.of(2015, 8, 30, 12, 36, 0).atZone(ZoneOffset.UTC)
    val srFuture =
      Signer.signedRequest(req, signingKey(date)).runWith(Sink.head)
    whenReady(srFuture) { signedRequest =>
      signedRequest should equal(
        HttpRequest(HttpMethods.GET)
          .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")
          .withHeaders(
            Host("iam.amazonaws.com"),
            RawHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8"),
            RawHeader("x-amz-date", "20150830T123600Z"),
            RawHeader("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            RawHeader(
              "Authorization",
              "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=dd479fa8a80364edf2119ec24bebde66712ee9c9cb2b0d92eb3ab9ccdc0c3947"
            )
          )
      )
    }
  }

  it should "format x-amz-date based on year-of-era instead of week-based-year" in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")

    val date = LocalDateTime.of(2017, 12, 31, 12, 36, 0).atZone(ZoneOffset.UTC)
    val srFuture =
      Signer.signedRequest(req, signingKey(date)).runWith(Sink.head)

    whenReady(srFuture) { signedRequest =>
      signedRequest.getHeader("x-amz-date").get.value should equal("20171231T123600Z")
    }
  }

  it should "add the correct security token header when session credentials are used" in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")

    val date = LocalDateTime.of(2017, 12, 31, 12, 36, 0).atZone(ZoneOffset.UTC)
    val sessionCredentials = AwsSessionCredentials.create(
      "AKIDEXAMPLE",
      "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
      "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3zrkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtpZ3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE"
    )

    val sessionCredentialsProvider = StaticCredentialsProvider.create(sessionCredentials)
    val key = SigningKey(date, sessionCredentialsProvider, CredentialScope(date.toLocalDate, Region.US_EAST_1, "iam"))

    val srFuture =
      Signer.signedRequest(req, key).runWith(Sink.head)

    whenReady(srFuture) { signedRequest =>
      signedRequest.getHeader("x-amz-security-token").get.value should equal(sessionCredentials.sessionToken)
    }
  }

  it should "correctly sign request with Raw-Request-URI header" in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")
      .withHeaders(
        Host("iam.amazonaws.com"),
        RawHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8"),
        `Raw-Request-URI`("/1 + 2=3")
      )

    val date = LocalDateTime.of(2015, 8, 30, 12, 36, 0).atZone(ZoneOffset.UTC)
    val srFuture = Signer.signedRequest(req, signingKey(date)).runWith(Sink.head)

    whenReady(srFuture) { signedRequest =>
      signedRequest.getHeader("Authorization").asScala.value shouldEqual RawHeader(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=dd479fa8a80364edf2119ec24bebde66712ee9c9cb2b0d92eb3ab9ccdc0c3947"
      )
    }
  }
}
