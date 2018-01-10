/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object SqsBatchFlowSettings {
  val Defaults = SqsBatchFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )
}

//#SqsBatchFlowSettings
final case class SqsBatchFlowSettings(maxBatchSize: Int, maxBatchWait: FiniteDuration, concurrentRequests: Int) {
  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )
}
//#SqsBatchFlowSettings
