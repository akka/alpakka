/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl
import akka.annotation.InternalApi

/**
 * Internal Api
 */
@InternalApi sealed trait S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object GetObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object HeadObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object PutObject extends S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object InitiateMultipartUpload extends S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object UploadPart extends S3Request

/**
 * Internal Api
 */
@InternalApi private[impl] case object CopyPart extends S3Request
