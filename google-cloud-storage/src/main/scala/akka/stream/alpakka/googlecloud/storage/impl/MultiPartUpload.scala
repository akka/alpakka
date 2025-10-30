/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.annotation.InternalApi

@InternalApi
private[impl] final case class MultiPartUpload(uploadId: String)
