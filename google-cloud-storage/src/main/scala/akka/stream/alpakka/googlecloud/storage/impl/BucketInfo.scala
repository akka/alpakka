/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.annotation.InternalApi

@InternalApi
private[impl] final case class BucketInfo(name: String, location: String)
