/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol.{AccountId, ApplicationKey, BucketId}
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import org.scalatest.AsyncFlatSpec
import org.scalatest.Matchers._

import scala.concurrent.duration._
import scala.concurrent.Await

class B2ClientSpec extends AsyncFlatSpec {
  private def readEnv(key: String): String = {
    Option(System.getenv(key)) getOrElse sys.error(s"Please set $key environment variable to run the tests")
  }

  private val accountId = AccountId(readEnv("B2_ACCOUNT_ID"))
  private val applicationKey = ApplicationKey(readEnv("B2_APPLICATION_KEY"))
  private val bucketId = BucketId(readEnv("B2_BUCKET_ID"))

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  private val client = new B2Client
  private val timeout = 10.seconds

  it should "authorize account" in {
    val result = client.authorizeAccount(accountId, applicationKey)
    result map { result =>
      result.authorizationToken.value should not be empty
      result.apiUrl.value should not be empty
      result.accountId.value should not be empty
    }
  }

  it should "get upload url" in {
    val authorizationF = client.authorizeAccount(accountId, applicationKey)
    val authorization = Await.result(authorizationF, timeout)

    val result = client.getUploadUrl(authorization.apiUrl, bucketId, authorization.authorizationToken)
    result map { result =>
      result.authorizationToken.value should not be empty
      result.uploadUrl.value should not be empty
      result.bucketId.value should not be empty
    }
  }
}
