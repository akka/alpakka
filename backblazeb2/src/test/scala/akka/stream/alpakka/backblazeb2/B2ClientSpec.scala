/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import java.nio.charset.StandardCharsets
import java.util.UUID
import cats.syntax.option._
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
  private val bucketName = BucketName("alpakka-test") // TODO: create new using b2_create_bucket then remove after
  private val bucketId = BucketId(readEnv("B2_BUCKET_ID")) // TODO: read using b2_list_buckets API call

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  private val credentials = Credentials(accountId, applicationKey)
  private val client = new B2Client(credentials)
  private val timeout = 10.seconds

  private val testRun = UUID.randomUUID().toString
  private val fileName = FileName(s"test-$testRun.txt")

  private def checkData(obtained: ByteString, expected: String) = {
    val receivedText = new String(obtained.toArray, StandardCharsets.UTF_8.name)
    receivedText shouldEqual expected
  }

  it should "work in happy case" in {
    val authorizationResultF = client.authorizeAccount()
    val authorizationResult = Await.result(authorizationResultF, timeout)
    authorizationResult.authorizationToken.value should not be empty
    authorizationResult.apiUrl.value should not be empty
    authorizationResult.accountId.value should not be empty

    val apiUrl = authorizationResult.apiUrl
    val accountAuthorization = authorizationResult.authorizationToken

    val text = "this is test data"
    val data = ByteString(text.getBytes(StandardCharsets.UTF_8.name))

    val getUploadUrlF = client.getUploadUrl(apiUrl, bucketId, accountAuthorization)
    val getUploadUrl = Await.result(getUploadUrlF, timeout)

    getUploadUrl.authorizationToken.value should not be empty
    getUploadUrl.uploadUrl.value should not be empty

    val uploadResultF = client.uploadFile(
      uploadUrl = getUploadUrl.uploadUrl,
      fileName = fileName,
      data = data,
      uploadAuthorization = getUploadUrl.authorizationToken,
      contentType = ContentTypes.`text/plain(UTF-8)`
    )

    val uploadResult = Await.result(uploadResultF, timeout)
    uploadResult.fileId.value should not be empty

    val fileId = uploadResult.fileId

    val downloadByNameResultF = client.downloadFileByName(fileName, bucketName, apiUrl, accountAuthorization.some)
    val downloadByNameResult = Await.result(downloadByNameResultF, timeout)
    checkData(downloadByNameResult, text)

    val downloadByIdResultF = client.downloadFileById(uploadResult.fileId, apiUrl, accountAuthorization.some)
    val downloadByIdResult = Await.result(downloadByIdResultF, timeout)
    checkData(downloadByIdResult, text)

    val deleteResultF = client.delete(bucketId, fileId, fileName, apiUrl, accountAuthorization)
    val deleteResult = Await.result(deleteResultF, timeout)
    deleteResult shouldEqual FileVersionInfo(uploadResult.fileName, uploadResult.fileId) :: Nil
  }
}
