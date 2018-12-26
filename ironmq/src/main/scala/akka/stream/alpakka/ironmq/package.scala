/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka
import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

package object ironmq {

  /**
   * This is a utility implicit class to allow convert a [[java.time.Duration]] in a [[scala.concurrent.duration.FiniteDuration FiniteDuration]].
   */
  implicit class RichJavaDuration(d: JDuration) {
    def asScala: FiniteDuration = {
      val seconds = d.getSeconds
      val nanos = d.getNano

      FiniteDuration(seconds, TimeUnit.SECONDS) + FiniteDuration(nanos, TimeUnit.NANOSECONDS)
    }
  }
}
