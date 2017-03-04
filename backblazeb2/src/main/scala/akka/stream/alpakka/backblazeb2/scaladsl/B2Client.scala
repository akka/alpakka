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
import akka.stream.alpakka.backblazeb2.{B2Encoder, B2Exception}
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.JsonSupport._
import akka.util.ByteString
import scala.concurrent.Future
import cats.syntax.either._

class B2Client(implicit system: ActorSystem, materializer: Materializer) {
  implicit val executionContext = materializer.executionContext
  private val version = "b2api/v1"
  private val DefaultContentType = ContentType.parse("b2/x-auto") getOrElse sys.error("Failed to parse b2/x-auto")

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

    requestAndParse[AuthorizeAccountResponse](request)
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
    val headers = RawHeader("Authorization", accountAuthorizationToken.value)
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(headers)

    requestAndParse[GetUploadUrlResponse](request)
  }

  /**
    * Upload a file
    */
  def upload(
    apiUrl: ApiUrl,
    bucketId: BucketId,
    accountAuthorizationToken: AccountAuthorizationToken,
    fileName: FileName,
    data: ByteString,
    contentType: ContentType = DefaultContentType
  ): Future[UploadFileResponse] = {
    val uploadUrlResponse = getUploadUrl(apiUrl, bucketId, accountAuthorizationToken)
    uploadUrlResponse flatMap { uploadUrlResponse =>
      uploadFile(uploadUrlResponse.uploadUrl, fileName, data, uploadUrlResponse.authorizationToken, contentType)
    }
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_upload_file.html
    */
  def uploadFile(
    uploadUrl: UploadUrl,
    fileName: FileName,
    data: ByteString,
    authorizationToken: UploadAuthorizationToken,
    contentType: ContentType = DefaultContentType
  ): Future[UploadFileResponse] = {
    val uri = Uri(uploadUrl.value)
    val headers =
      RawHeader("Authorization", authorizationToken.value) ::
      RawHeader("X-Bz-File-Name", B2Encoder.encode(fileName.value)) ::
      RawHeader("X-Bz-Content-Sha1", B2Encoder.sha1String(data)) ::
      Nil

    val entity = HttpEntity(data).withContentType(contentType)

    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.POST
    ).withHeadersAndEntity(headers, entity)

    requestAndParse[UploadFileResponse](request)
  }

  private def requestAndParse[T : FromEntityUnmarshaller](request: HttpRequest): Future[T] = {
    Http().singleRequest(request) flatMap { response =>
      parseResponse[T](response)
        .recover { case t: Throwable => // this adds useful debug info to the error
          throw new RuntimeException(s"Failed to decode $response for $request", t)
        }
    }
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_download_file_by_name.html
    */
  def downloadFileByName(
    fileName: FileName
  ): Future[DownloadFileByNameResponse] = {
    ???
  }

  /**
    * Delete all versions for a file identified by a filename.
    */
  def delete(
    fileName: FileName
  ): Future[List[FileId]] = {
    listFileVersions(fileName) flatMap { x =>
      Future.sequence(x map deleteFileVersion)
    }
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_list_file_versions.html
    */
  def listFileVersions(
    fileName: FileName
  ): Future[List[FileVersion]] = {
    ???
  }

  /**
    * https://www.backblaze.com/b2/docs/b2_delete_file_version.html
    */
  def deleteFileVersion(
    fileVersion: FileVersion
  ): Future[FileId] = {
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
        Unmarshal(entity).to[String].flatMap { result =>
          Future.failed(new B2Exception(s"HTTP error $status - $result"))
        }
    }
  }
}
