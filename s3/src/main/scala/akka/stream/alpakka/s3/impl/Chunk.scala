/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.impl

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString

private[alpakka] final case class Chunk(data: Source[ByteString, NotUsed], size: Int)
