/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.stream.alpakka.googlecloud.storage.StorageObject
import akka.annotation.InternalApi

@InternalApi
private[impl] final case class BucketListResult(
    kind: String,
    nextPageToken: Option[String],
    prefixes: Option[List[String]],
    items: List[StorageObject]
) {
  def merge(other: BucketListResult): BucketListResult =
    copy(nextPageToken = None, items = this.items ++ other.items, prefixes = for {
      source <- this.prefixes
      other <- other.prefixes
    } yield source ++ other)
}
