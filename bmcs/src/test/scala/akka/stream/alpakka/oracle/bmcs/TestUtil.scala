/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString

object TestUtil {

  val largeString: String = (0 to 10).map(_.toString * 1024).fold("")(_ + _)
  val largeSrc: Source[ByteString, NotUsed] =
    Source.fromIterator(() => (0 to 100).iterator).map(_ => ByteString(largeString))

  def largeSource(mb: Int = 1): Source[ByteString, NotUsed] =
    Source.fromIterator(() => (0 to mb).iterator).flatMapConcat(_ => largeSrc)

}
