package akka.stream.alpakka.s3.impl

import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.util.ByteString

case class Chunk(data: Source[ByteString, NotUsed], size: Int)
