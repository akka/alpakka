/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.impl

import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.annotation.InternalApi
import akka.util.ByteString

/**
 * Internal Api
 */
@InternalApi private[impl] final case class Chunk(data: Source[ByteString, NotUsed], size: Int)
