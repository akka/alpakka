/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

sealed abstract class InsertAllRetryPolicy {
  def retry: Boolean
  def deduplicate: Boolean
}

case object NoRetries extends InsertAllRetryPolicy {
  override def retry: Boolean = false
  override def deduplicate: Boolean = false
}

case object RetryNoDeduplication extends InsertAllRetryPolicy {
  override def retry: Boolean = true
  override def deduplicate: Boolean = false
}

case object RetryWithDeduplication extends InsertAllRetryPolicy {
  override def retry: Boolean = true
  override def deduplicate: Boolean = true
}
