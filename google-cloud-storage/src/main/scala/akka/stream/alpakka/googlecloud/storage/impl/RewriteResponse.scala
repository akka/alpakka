/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.alpakka.googlecloud.storage.StorageObject
import akka.annotation.InternalApi

@InternalApi
private[impl] final case class RewriteResponse(
    kind: String,
    totalBytesRewritten: Long,
    objectSize: Long,
    done: Boolean,
    rewriteToken: Option[String],
    resource: Option[StorageObject]
)
