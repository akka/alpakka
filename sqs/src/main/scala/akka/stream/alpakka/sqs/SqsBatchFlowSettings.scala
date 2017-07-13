/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object SqsBatchFlowSettings {
  val Defaults = SqsBatchFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequest = 1
  )
}
final case class SqsBatchFlowSettings(maxBatchSize: Int, maxBatchWait: FiniteDuration, concurrentRequest: Int) {}
