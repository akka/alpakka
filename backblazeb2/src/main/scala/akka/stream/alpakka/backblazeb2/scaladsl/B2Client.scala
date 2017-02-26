/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.stream.alpakka.backblazeb2.B2Encoder.encodeBase64
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.Materializer
import akka.stream.alpakka.backblazeb2.B2Exception
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.JsonSupport._
import akka.util.ByteString
import scala.concurrent.Future

class B2Client(implicit system: ActorSystem, materializer: Materializer) {
  implicit val executionContext = materializer.executionContext
  private val version = "b2api/v1"
  private val DefaultContentType = "b2/x-auto"

  /**
   * https://www.backblaze.com/b2/docs/b2_authorize_account.html
   */
  def authorizeAccount(accountId: AccountId, applicationKey: ApplicationKey): Future[AuthorizeAccountResponse] = {
    val encodedCredentials = encodeBase64(s"$accountId:$applicationKey")
    val authorization = s"Basic $encodedCredentials"
    val request = HttpRequest(
      uri = s"https://api.backblazeb2.com/$version/b2_authorize_account",
      method = HttpMethods.GET
    ).withHeaders(RawHeader("Authorization", authorization))

    for {
      response <- Http().singleRequest(request)
      result <- parseResponse[AuthorizeAccountResponse](response)
    } yield result
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_get_upload_url.html
    */
  def getUploadUrl(
    apiUrl: ApiUrl,
    bucketId: BucketId,
    accountAuthorizationToken: AccountAuthorizationToken
  ): Future[GetUploadUrlResponse] = {
    val uri = Uri(s"$apiUrl/$version/b2_get_upload_url").withQuery(Query("bucketId" -> bucketId.value))
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(RawHeader("Authorization", accountAuthorizationToken.value))

    for {
      response <- Http().singleRequest(request)
      result <- parseResponse[GetUploadUrlResponse](response)
    } yield result
  }

  def upload(
    apiUrl: ApiUrl,
    bucketId: BucketId,
    accountAuthorizationToken: AccountAuthorizationToken,
    fileName: FileName,
    data: ByteString,
    contentType: String = DefaultContentType
  ): Future[UploadFileResponse] = {
    val uploadUrlResponse = getUploadUrl(apiUrl, bucketId, accountAuthorizationToken)
    uploadUrlResponse flatMap { uploadUrlResponse =>
      uploadFile(uploadUrlResponse.uploadUrl, fileName, data, contentType)
    }
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_upload_file.html
    */
  def uploadFile(
    uploadUrl: UploadUrl,
    fileName: FileName,
    data: ByteString,
    contentType: String = DefaultContentType
  ): Future[UploadFileResponse] = {
    ???
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_download_file_by_name.html
    */
  def downloadFileByName(
    fileName: FileName
  ): Future[DownloadFileByNameResponse] = {
    ???
  }

  private def parseResponse[T : FromEntityUnmarshaller](response: HttpResponse): Future[T] = {
    for {
      entity <- entityForSuccess(response)
      result <- Unmarshal(entity).to[T]
    } yield result
  }

  private def entityForSuccess(response: HttpResponse): Future[ResponseEntity] = {
    response match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Future.successful(entity)

      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap(result => Future.failed(new B2Exception(s"HTTP error $status - $result")))
    }
  }
}
