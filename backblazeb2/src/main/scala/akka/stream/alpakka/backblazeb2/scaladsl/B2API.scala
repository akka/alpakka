/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.backblazeb2.scaladsl

import java.util.UUID

import akka.stream.alpakka.backblazeb2.B2Encoder.encodeBase64
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.stream.Materializer
import akka.stream.alpakka.backblazeb2.B2Encoder
import akka.stream.alpakka.backblazeb2.Protocol._
import akka.stream.alpakka.backblazeb2.SerializationSupport._
import akka.util.ByteString
import cats.data.EitherT
import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

object B2API {
  val DefaultHostAndPort = "api.backblazeb2.com"
}

/**
 * Handles the interface with the Backblaze B2 API, but doesn't handle any expired tokens or retries
 */
class B2API(hostAndPort: String = B2API.DefaultHostAndPort)(implicit system: ActorSystem, materializer: Materializer)
    extends LazyLogging {
  implicit val executionContext = materializer.executionContext
  private val version = "b2api/v1"

  /**
   * https://www.backblaze.com/b2/docs/b2_authorize_account.html
   */
  def authorizeAccount(credentials: B2AccountCredentials): B2Response[AuthorizeAccountResponse] = {
    val encodedCredentials = encodeBase64(s"${credentials.accountId}:${credentials.applicationKey}")
    val authorization = s"Basic $encodedCredentials"
    val request = HttpRequest(
      uri = s"https://$hostAndPort/$version/b2_authorize_account",
      method = HttpMethods.GET
    ).withHeaders(RawHeader("Authorization", authorization))

    requestAndParse[AuthorizeAccountResponse](request)
  }

  /**
   * https://www.backblaze.com/b2/docs/b2_get_upload_url.html
   */
  def getUploadUrl(
      authorizeAccountResponse: AuthorizeAccountResponse,
      bucketId: BucketId
  ): B2Response[GetUploadUrlResponse] = {
    val apiUrl = authorizeAccountResponse.apiUrl
    val accountAuthorization = authorizeAccountResponse.authorizationToken
    val uri = Uri(s"$apiUrl/$version/b2_get_upload_url").withQuery(Query("bucketId" -> bucketId.value))
    val headers = RawHeader("Authorization", accountAuthorization.value)
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(headers)

    requestAndParse[GetUploadUrlResponse](request)
  }

  /**
   * https://www.backblaze.com/b2/docs/b2_upload_file.html
   */
  def uploadFile(
      uploadCredentials: GetUploadUrlResponse,
      fileName: FileName,
      data: ByteString,
      contentType: ContentType = DefaultContentType
  ): B2Response[UploadFileResponse] = {
    val uri = Uri(uploadCredentials.uploadUrl.value)
    val headers =
    RawHeader("Authorization", uploadCredentials.authorizationToken.value) ::
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

  private def requestAndParse[T: FromResponseUnmarshaller](request: HttpRequest): B2Response[T] = {
    val requestId = UUID.randomUUID()
    val start = System.nanoTime()
    logger.trace(s"request $requestId: Invoking ${request.uri}")

    Http()
      .singleRequest(request)
      .map { x =>
        val end = System.nanoTime()
        val delta = (end - start) / 1000000
        logger.debug(s"request $requestId: Invoking ${request.uri} done in $delta ms with status ${x.status}")
        x
      }
      .flatMap { response =>
        parseResponse[T](response).recover {
          case t: Throwable => // this adds useful debug info to the error
            throw new RuntimeException(s"Failed to decode $response for $request", t)
        }
      }
  }

  private def authorizationHeaders(authorization: Option[AccountAuthorizationToken]) =
    authorization.map(authorizationHeader).toSeq

  private def authorizationHeader(authorization: AccountAuthorizationToken) =
    RawHeader("Authorization", authorization.value)

  /**
   * https://www.backblaze.com/b2/docs/b2_download_file_by_name.html
   */
  def downloadFileByName(
      fileName: FileName,
      bucketName: BucketName,
      apiUrl: ApiUrl,
      accountAuthorization: Option[AccountAuthorizationToken]
  ): B2Response[DownloadFileResponse] = {
    val uri = Uri(s"$apiUrl/file/$bucketName/$fileName")
    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(authorizationHeaders(accountAuthorization): _*)

    requestAndParse[DownloadFileResponse](request)
  }

  /**
   * https://www.backblaze.com/b2/docs/b2_download_file_by_id.html
   */
  def downloadFileById(
      fileId: FileId,
      apiUrl: ApiUrl,
      accountAuthorization: Option[AccountAuthorizationToken]
  ): B2Response[DownloadFileResponse] = {
    val uri = Uri(s"$apiUrl/b2api/v1/b2_download_file_by_id").withQuery(Query("fileId" -> fileId.value))

    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(authorizationHeaders(accountAuthorization): _*)

    requestAndParse[DownloadFileResponse](request)
  }

  /**
   * https://www.backblaze.com/b2/docs/b2_list_file_versions.html
   */
  def listFileVersions(
      bucketId: BucketId,
      startFileId: Option[FileId],
      startFileName: Option[FileName],
      maxFileCount: Int,
      apiUrl: ApiUrl,
      accountAuthorization: AccountAuthorizationToken
  ): B2Response[ListFileVersionsResponse] = {
    val query =
    List(
      "bucketId" -> bucketId.value,
      "maxFileCount" -> maxFileCount.toString
    ) ++
    startFileId.map { "startFileId" -> _.value }.toList ++
    startFileName.map { "startFileName" -> _.value }.toList

    val uri = Uri(s"$apiUrl/b2api/v1/b2_list_file_versions").withQuery(Query(query: _*))

    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(authorizationHeader(accountAuthorization))

    requestAndParse[ListFileVersionsResponse](request)
  }

  /**
   * https://www.backblaze.com/b2/docs/b2_delete_file_version.html
   */
  def deleteFileVersion(
      fileVersion: FileVersionInfo,
      apiUrl: ApiUrl,
      accountAuthorization: AccountAuthorizationToken
  ): B2Response[FileVersionInfo] = {
    val uri = Uri(s"$apiUrl/b2api/v1/b2_delete_file_version").withQuery(
      Query(
        "fileId" -> fileVersion.fileId.value,
        "fileName" -> fileVersion.fileName.value
      )
    )

    val request = HttpRequest(
      uri = uri,
      method = HttpMethods.GET
    ).withHeaders(authorizationHeader(accountAuthorization))

    requestAndParse[FileVersionInfo](request)
  }

  private def parseResponse[T: FromResponseUnmarshaller](response: HttpResponse): B2Response[T] = {
    import cats.implicits._
    val result = for {
      reponse <- EitherT(ensureSuccessfulResponse(response))
      unmarshalled <- EitherT(Unmarshal(reponse).to[T].map(x => x.asRight[B2Error]))
    } yield unmarshalled

    result.value
  }

  private def ensureSuccessfulResponse(response: HttpResponse): B2Response[HttpResponse] =
    response match {
      case x @ HttpResponse(status, _, _, _) if status.isSuccess() =>
        Future.successful(x.asRight)

      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[B2ErrorResponse].flatMap { result =>
          require(status.intValue == result.status,
                  s"Expected statuses to match but got $status from HTTP response but ${result.status} in JSON")
          Future.successful(B2Error(status, result.code, result.message).asLeft)
        }
    }
}
