/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.logging.model.LogEntry
import akka.stream.scaladsl.Flow

import java.util.{SplittableRandom, UUID}

@InternalApi
private[logging] object WithInsertId {

  def apply[T]: Flow[LogEntry[T], LogEntry[T], NotUsed] = Flow[LogEntry[T]].statefulMapConcat { () =>
    val random = new SplittableRandom()
    e => e.withInsertId(randomUUID(random).toString) :: Nil
  }

  private def randomUUID(randomGen: SplittableRandom): UUID = {
    var msb = randomGen.nextLong()
    var lsb = randomGen.nextLong()
    msb &= 0xFFFFFFFFFFFF0FFFL // clear version
    msb |= 0x0000000000004000L // set to version 4
    lsb &= 0x3FFFFFFFFFFFFFFFL // clear variant
    lsb |= 0x8000000000000000L // set to IETF variant
    new UUID(msb, lsb)
  }
}
