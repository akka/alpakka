/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.alpakka.googlecloud.storage.StorageObject
import akka.annotation.InternalApi

@InternalApi
private[impl] sealed trait UploadPartResponse

@InternalApi
private[impl] final case class SuccessfulUploadPart(multiPartUpload: MultiPartUpload, index: Int)
    extends UploadPartResponse
@InternalApi
private[impl] final case class FailedUploadPart(multiPartUpload: MultiPartUpload, index: Int, exception: Throwable)
    extends UploadPartResponse
@InternalApi
private[impl] final case class SuccessfulUpload(multiPartUpload: MultiPartUpload,
                                                index: Int,
                                                storageObject: StorageObject)
    extends UploadPartResponse
