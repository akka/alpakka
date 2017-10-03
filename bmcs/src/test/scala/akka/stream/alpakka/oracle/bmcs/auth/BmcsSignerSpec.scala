/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.auth

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{Date, Host, RawHeader}
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

class BmcsSignerSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with ScalaFutures {

  def this() = this(ActorSystem("BmcsSignerSpec"))

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system).withDebugLogging(true)
  )

  val region = "us-phoenix-1"
  val userOcid = "ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq"
  val tenancyOcid = "ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa"
  val keyPath = "./bmcs/src/test/resources/oci_api_key.pem"
  val passphrase = "adityag"
  val keyFingerprint = "b4:97:75:d0:2a:40:1f:5e:66:a3:f4:03:9a:ff:a8:8a"

  val credentials: BmcsCredentials = BasicCredentials(userOcid, tenancyOcid, keyPath, Some(passphrase), keyFingerprint)

  "BmcsSigner" should "sign a GET request correctly " in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://objectstorage.us-phoenix-1.oraclecloud.com/n/oraclegbudev/b/CEGBU_Prime/o/TestObject.pdf")
      .withHeaders(
        Host("objectstorage.us-phoenix-1.oraclecloud.com"),
        RawHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
      )

    val date = Instant.parse("2017-09-20T17:32:37Z")
    val signed = BmcsSigner.signedRequest(req, credentials, date = date)
    val expectedSignatureString =
      """Signature version="1",headers="host date (request-target)",keyId="ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa/ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq/b4:97:75:d0:2a:40:1f:5e:66:a3:f4:03:9a:ff:a8:8a",algorithm="rsa-sha256",signature="QGswwJGp6JwbbCQtcEYzbyjwlsSRRa4RPqvIrBtRGdLZBIHR9rbxbn8ld+u937OSUF9gRv3OkpMB8+XGzLnLRDaZaGrFIWoiZ8Ccup4aSUAl0WvdXOyy0UYCtIfo7E/0jqjVed3UxNZag01ah3DysfP743Wkzv3TQ/zD709OnCD6+Nfu/CEucV0eieyLdrW7Mj7juJl5IBZTPezKh6EPhkiF97yGSXzm+aENKfpoYYMKJNOYRHlrgp/C6DEiwD9yItajmSPetctnneYsMpQTugKXenwdzElj5LQ9Zsk2Z7lHZfx8w/CPAT0XU7mhesSBsdhCTN+WS48xnIjdL0GOAA==""""
    whenReady(signed) { signedRequest =>
      signedRequest should equal(
        HttpRequest(HttpMethods.GET)
          .withUri("https://objectstorage.us-phoenix-1.oraclecloud.com/n/oraclegbudev/b/CEGBU_Prime/o/TestObject.pdf")
          .withHeaders(
            Host("objectstorage.us-phoenix-1.oraclecloud.com"),
            RawHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8"),
            Date(DateTime(date.toEpochMilli)),
            RawHeader(
              "Authorization",
              expectedSignatureString
            )
          )
      )
    }

  }

  it should "sign a POST request correctly" in {
    val entityJson = """{"object":"NewBuffersTry10.zip"}"""
    val req = HttpRequest(HttpMethods.POST)
      .withUri("https://objectstorage.us-phoenix-1.oraclecloud.com/n/oraclegbudev/b/CEGBU_Prime/u")
      .withHeaders(
        Host("objectstorage.us-phoenix-1.oraclecloud.com")
      )
      .withEntity(ContentTypes.`application/json`, entityJson)

    val date = Instant.parse("2017-09-20T17:32:37Z")
    val expectedSignatureString =
      """Signature version="1",headers="host x-content-sha256 date content-type content-length (request-target)",keyId="ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa/ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq/b4:97:75:d0:2a:40:1f:5e:66:a3:f4:03:9a:ff:a8:8a",algorithm="rsa-sha256",signature="hvN0owJSgrwIemEx9canPBR3FCN58F6mI1zyl0cBIgW1MdkTewaGfiWT/qo3aShxgZlDHKTUIgvte8AIIRNw4TojP5txujg8hD/EEdYDf8/GuJwg3zIzCIpj85DvTlRddwnfb7QW8cbIpGkayuyIbfyuAFP7ExDKL7MnpdA9mmr9fDadoJQi9UmlVV0QXou7xZg2Z7Tp5AE8jB131O213TLffszIkZc741EJDwcQvn9RXjDHbVl0qwP6LXbO+UarZ2aeT4wxqlAIX37VYC2Y8ejtNPccji9Qy8Vc8Kpyb1CaaXxdHlh0sM3UdsUUJEdNbtMMCH1/bvQ2ZKMfqKCB/Q==""""

    val signed = BmcsSigner.signedRequest(req, credentials, date = date)
    whenReady(signed) { signedRequest =>
      signedRequest should equal(
        HttpRequest(HttpMethods.POST)
          .withUri("https://objectstorage.us-phoenix-1.oraclecloud.com/n/oraclegbudev/b/CEGBU_Prime/u")
          .withHeaders(
            Host("objectstorage.us-phoenix-1.oraclecloud.com"),
            RawHeader("x-content-sha256", "6so+0JPtKwndNimvAPDRevxPwFlvzdpdLulpDVt/Qg4="),
            Date(DateTime(date.toEpochMilli)),
            RawHeader(
              "Authorization",
              expectedSignatureString
            )
          )
          .withEntity(ContentTypes.`application/json`, entityJson)
      )
    }

  }

  it should "throw an error for CONNECT request " in {
    val req = HttpRequest(HttpMethods.CONNECT)
      .withUri("https://objectstorage.us-phoenix-1.oraclecloud.com/n/oraclegbudev/b/CEGBU_Prime/u")
      .withHeaders(
        Host("objectstorage.us-phoenix-1.oraclecloud.com")
      )
    val caught = intercept[UnsupportedOperationException] {
      BmcsSigner.signedRequest(req, credentials)
    }
    assert(caught.getMessage === "Does not support this httpMethod: CONNECT")
  }

}
