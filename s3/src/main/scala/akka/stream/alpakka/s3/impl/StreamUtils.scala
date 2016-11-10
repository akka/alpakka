package akka.stream.alpakka.s3.impl

import akka.NotUsed
import akka.stream.scaladsl.Source

object StreamUtils {
  def counter(initial: Int = 0): Source[Int, NotUsed] = {
    Source.unfold(initial)((i: Int) => Some((i + 1, i)))
  }
}
