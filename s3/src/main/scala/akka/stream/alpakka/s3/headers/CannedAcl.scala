/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.headers

import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.RawHeader

/**
 * Documentation: http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
 */
final class CannedAcl private (val value: String) {
  @InternalApi private[s3] def header: HttpHeader = RawHeader("x-amz-acl", value)
}

object CannedAcl {
  val AuthenticatedRead = new CannedAcl("authenticated-read")
  val AwsExecRead = new CannedAcl("aws-exec-read")
  val BucketOwnerFullControl = new CannedAcl("bucket-owner-full-control")
  val BucketOwnerRead = new CannedAcl("bucket-owner-read")
  val Private = new CannedAcl("private")
  val PublicRead = new CannedAcl("public-read")
  val PublicReadWrite = new CannedAcl("public-read-write")
}
