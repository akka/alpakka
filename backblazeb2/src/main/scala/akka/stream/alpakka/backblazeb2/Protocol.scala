/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

import akka.http.scaladsl.model.StatusCode

import scala.concurrent.Future

object Protocol {
  type B2Response[T] = Future[Either[B2Error, T]]

  /** Representation of a B2 Error */
  case class B2Error(statusCode: StatusCode, code: String, message: String)

  /** https://www.backblaze.com/b2/docs/calling.html#error_handling */
  case class B2ErrorResponse(status: Int, code: String, message: String)

  case class B2AccountCredentials(accountId: AccountId, applicationKey: ApplicationKey)

  case class AccountAuthorizationToken(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class UploadAuthorizationToken(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class ApplicationKey(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class AccountId(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class BucketId(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class UploadUrl(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class ApiUrl(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class FileId(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class FileName(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class BucketName(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class Sha1(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class AuthorizeAccountResponse(
    accountId: AccountId,
    apiUrl: ApiUrl,
    authorizationToken: AccountAuthorizationToken
  )

  case class GetUploadUrlResponse(
    bucketId: BucketId,
    uploadUrl: UploadUrl,
    authorizationToken: UploadAuthorizationToken
  )

  case class UploadFileResponse(
    fileId: FileId,
    fileName: FileName,
    accountId: AccountId,
    bucketId: BucketId,
    contentLength: Long,
    contentSha1: Sha1,
    contentType: String,
    fileInfo: Map[String, String]
  )

  case class ListFileVersionsResponse(
    files: List[FileVersionInfo]
  )

  case class FileVersionInfo(
    fileName: FileName,
    fileId: FileId
  )
}
