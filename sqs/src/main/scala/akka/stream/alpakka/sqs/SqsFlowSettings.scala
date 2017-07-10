/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object SqsFlowSettings {
  val Defaults = SqsFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequest = 1
  )
}
final case class SqsFlowSettings(maxBatchSize: Int, maxBatchWait: FiniteDuration, concurrentRequest: Int) {}
