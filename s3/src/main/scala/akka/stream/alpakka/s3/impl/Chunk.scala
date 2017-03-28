/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.util.ByteString

private[alpakka] final case class Chunk(data: Source[ByteString, NotUsed], size: Int)
