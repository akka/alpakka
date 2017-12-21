/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

trait S3Request

case object GetObject extends S3Request

case object HeadObject extends S3Request

case object PutObject extends S3Request

case object InitiateMultipartUpload extends S3Request

case object UploadPart extends S3Request
