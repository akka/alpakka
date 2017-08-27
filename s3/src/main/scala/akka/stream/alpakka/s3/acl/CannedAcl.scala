/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.acl

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
 */
sealed abstract class CannedAcl(val value: String) {
  def header: HttpHeader = RawHeader("x-amz-acl", value)
}

object CannedAcl {
  case object AuthenticatedRead extends CannedAcl("authenticated-read")
  case object AwsExecRead extends CannedAcl("aws-exec-read")
  case object BucketOwnerFullControl extends CannedAcl("bucket-owner-full-control")
  case object BucketOwnerRead extends CannedAcl("bucket-owner-read")
  case object Private extends CannedAcl("private")
  case object PublicRead extends CannedAcl("public-read")
  case object PublicReadWrite extends CannedAcl("public-read-write")
}
