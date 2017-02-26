/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

object Protocol {
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

  case class DownloadFileByNameResponse(
    // TODO
  )
}
