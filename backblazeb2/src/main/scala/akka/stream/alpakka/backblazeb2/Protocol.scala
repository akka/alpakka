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

  case class GetUploadUrlResponse(
    bucketId: BucketId,
    uploadUrl: UploadUrl,
    authorizationToken: UploadAuthorizationToken
  )

  case class AuthorizeAccountResponse(
    accountId: AccountId,
    apiUrl: ApiUrl,
    authorizationToken: AccountAuthorizationToken
  )
}
