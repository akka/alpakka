/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.scaladsl.B2Client
import akka.util.ByteString
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

  private val testRun = UUID.randomUUID().toString
  private val fileName = FileName(s"test-$testRun.txt")

  it should "work in happy case" in {
    val authorizationResultF = client.authorizeAccount(accountId, applicationKey)
    val authorizationResult = Await.result(authorizationResultF, timeout)
    authorizationResult.authorizationToken.value should not be empty
    authorizationResult.apiUrl.value should not be empty
    authorizationResult.accountId.value should not be empty

    val text = "this is test data"
    val data = ByteString(text.getBytes(StandardCharsets.UTF_8.name))
    val uploadResultF = client.upload(
      apiUrl = authorizationResult.apiUrl,
      bucketId = bucketId,
      accountAuthorizationToken = authorizationResult.authorizationToken,
      fileName = fileName,
      data = data,
      contentType = ContentTypes.`text/plain(UTF-8)`
    )

    val uploadResult = Await.result(uploadResultF, timeout)
    uploadResult.fileId.value should not be empty

    val downloadResultF = client.downloadFileByName(fileName)
    val downloadResult = Await.result(downloadResultF, timeout)
    downloadResult.data shouldEqual data
    val receivedText = new String(downloadResult.data.toArray, StandardCharsets.UTF_8.name)
    receivedText shouldEqual text

    val deleteResultF = client.delete(fileName)
    val deleteResult = Await.result(deleteResultF, timeout)
    deleteResult shouldEqual uploadResult.fileId :: Nil
  }
}
