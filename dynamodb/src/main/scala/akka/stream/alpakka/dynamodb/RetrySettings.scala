/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb
import akka.stream.alpakka.dynamodb.RetrySettings.RetryBackoffStrategy

import scala.concurrent.duration._

object RetrySettings {
  sealed trait RetryBackoffStrategy
  case object Exponential extends RetryBackoffStrategy
  case object Linear extends RetryBackoffStrategy

  val DefaultRetrySettings = RetrySettings(3, 1.seconds, Exponential)
}

case class RetrySettings(maximumRetries: Int,
                         initialRetryTimeout: FiniteDuration,
                         backoffStrategy: RetryBackoffStrategy)
