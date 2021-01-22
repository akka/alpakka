/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

sealed abstract class InsertAllRetryPolicy {
  def retry: Boolean
  def deduplicate: Boolean
  final def getInstance: InsertAllRetryPolicy = this;
}

/**
 * Never retry failed insert requests
 */
case object NoRetries extends InsertAllRetryPolicy {
  override def retry: Boolean = false
  override def deduplicate: Boolean = false
}

/**
 * Retry failed insert requests without deduplication
 */
case object RetryNoDeduplication extends InsertAllRetryPolicy {
  override def retry: Boolean = true
  override def deduplicate: Boolean = false
}

/**
 * Retry failed insert requests with best-effort deduplication
 */
case object RetryWithDeduplication extends InsertAllRetryPolicy {
  override def retry: Boolean = true
  override def deduplicate: Boolean = true
}
