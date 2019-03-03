/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl
import akka.annotation.InternalApi

/**
 * Internal Api
 */
@InternalApi private[s3] sealed trait S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object GetObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object HeadObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object PutObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object InitiateMultipartUpload extends S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object UploadPart extends S3Request

/**
 * Internal Api
 */
@InternalApi private[s3] case object CopyPart extends S3Request
