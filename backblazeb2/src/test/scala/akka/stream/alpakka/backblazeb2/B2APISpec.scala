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
import akka.stream.alpakka.backblazeb2.scaladsl.B2API
import akka.util.ByteString
import org.scalatest.AsyncFlatSpec
import org.scalatest.Matchers._

/** Integration test, requires access to B2 and configured B2_ACCOUNT_ID and B2_APPLICATION_KEY environment variables */
class B2APISpec extends AsyncFlatSpec with B2IntegrationTest {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  private val api = new B2API()

  private val testRun = UUID.randomUUID().toString
  private val fileName = FileName(s"test-$testRun.txt")

  private def checkData(obtained: ByteString, expected: String) = {
    val receivedText = new String(obtained.toArray, StandardCharsets.UTF_8.name)
    receivedText shouldEqual expected
  }

  it should "work in happy case" in {
    val authorizationResultF = api.authorizeAccount(credentials)
    val authorizationResult = extractFromResponse(authorizationResultF)
    authorizationResult.authorizationToken.value should not be empty
    authorizationResult.apiUrl.value should not be empty
    authorizationResult.accountId.value should not be empty

    val apiUrl = authorizationResult.apiUrl
    val accountAuthorization = authorizationResult.authorizationToken

    val text = "This is test data!"
    val data = ByteString(text.getBytes(StandardCharsets.UTF_8.name))

    val getUploadUrlF = api.getUploadUrl(authorizationResult, bucketId)
    val getUploadUrl = extractFromResponse(getUploadUrlF)

    getUploadUrl.authorizationToken.value should not be empty
    getUploadUrl.uploadUrl.value should not be empty

    val uploadResultF = api.uploadFile(
      uploadCredentials = getUploadUrl,
      fileName = fileName,
      data = data,
      contentType = ContentTypes.`text/plain(UTF-8)`
    )

    val uploadResult = extractFromResponse(uploadResultF)
    uploadResult.fileId.value should not be empty

    val fileId = uploadResult.fileId

    val downloadByNameResultF = api.downloadFileByName(fileName, bucketName, apiUrl, accountAuthorization.some)
    val downloadByNameResult = extractFromResponse(downloadByNameResultF)
    checkData(downloadByNameResult, text)

    val downloadByIdResultF = api.downloadFileById(uploadResult.fileId, apiUrl, accountAuthorization.some)
    val downloadByIdResult = extractFromResponse(downloadByIdResultF)
    checkData(downloadByIdResult.data, text)

    val deleteResultF = api.deleteFileVersion(FileVersionInfo(fileName, fileId), apiUrl, accountAuthorization)
    val deleteResult = extractFromResponse(deleteResultF)
    deleteResult shouldEqual FileVersionInfo(uploadResult.fileName, uploadResult.fileId)
  }
}
