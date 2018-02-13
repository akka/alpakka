/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.alpakka.backblazeb2.Protocol._
import cats.syntax.option._
import org.scalatest.AsyncFlatSpec
import org.scalatest.Matchers._

/** Integration test, requires access to B2 and configured B2_ACCOUNT_ID and B2_APPLICATION_KEY environment variables */
class B2APIIntegrationSpec extends AsyncFlatSpec with B2IntegrationTest {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  private val api = new B2API()

  it should "work in happy case" in {
    val authorizationResultF = api.authorizeAccount(credentials)
    val authorizationResult = extractFromResponse(authorizationResultF)
    authorizationResult.authorizationToken.value should not be empty
    authorizationResult.apiUrl.value should not be empty
    authorizationResult.accountId.value should not be empty

    val apiUrl = authorizationResult.apiUrl
    val accountAuthorization = authorizationResult.authorizationToken

    val getUploadUrlF = api.getUploadUrl(authorizationResult, bucketId)
    val getUploadUrl = extractFromResponse(getUploadUrlF)

    getUploadUrl.authorizationToken.value should not be empty
    getUploadUrl.uploadUrl.value should not be empty

    val uploadResultF = api.uploadFile(
      uploadCredentials = getUploadUrl,
      fileName = fileName,
      data = dataByteString,
      contentType = ContentTypes.`text/plain(UTF-8)`
    )

    val uploadResult = extractFromResponse(uploadResultF)
    uploadResult.fileId.value should not be empty

    val fileId = uploadResult.fileId

    val fileVersionsResponseF = api.listFileVersions(
      bucketId,
      startFileId = None,
      startFileName = fileName.some,
      maxFileCount = 1,
      apiUrl = apiUrl,
      accountAuthorization = accountAuthorization
    )
    val fileVersionsResponse = extractFromResponse(fileVersionsResponseF)
    fileVersionsResponse.files shouldEqual List(FileVersionInfo(fileName, fileId))

    val downloadByNameResultF = api.downloadFileByName(fileName, bucketName, apiUrl, accountAuthorization.some)
    val downloadByNameResult = extractFromResponse(downloadByNameResultF)
    checkDownloadResponse(downloadByNameResult)

    val downloadByIdResultF = api.downloadFileById(uploadResult.fileId, apiUrl, accountAuthorization.some)
    val downloadByIdResult = extractFromResponse(downloadByIdResultF)
    checkDownloadResponse(downloadByIdResult)

    val deleteResultF = api.deleteFileVersion(FileVersionInfo(fileName, fileId), apiUrl, accountAuthorization)
    val deleteResult = extractFromResponse(deleteResultF)
    deleteResult shouldEqual FileVersionInfo(uploadResult.fileName, uploadResult.fileId)
  }
}
