/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery

sealed abstract class InsertAllRetryPolicy {
  def retry: Boolean
  def deduplicate: Boolean
}

object InsertAllRetryPolicy {

  /**
   * Never retry failed insert requests
   */
  case object Never extends InsertAllRetryPolicy {
    override def retry: Boolean = false
    override def deduplicate: Boolean = false
  }

  /**
   * Java API: Never retry failed insert requests
   */
  def never = Never

  /**
   * Retry failed insert requests without deduplication
   */
  case object WithoutDeduplication extends InsertAllRetryPolicy {
    override def retry: Boolean = true
    override def deduplicate: Boolean = false
  }

  /**
   * Java API: Retry failed insert requests without deduplication
   */
  def withoutDeduplication = WithDeduplication

  /**
   * Retry failed insert requests with best-effort deduplication
   * @see [[https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency BigQuery reference]]
   */
  case object WithDeduplication extends InsertAllRetryPolicy {
    override def retry: Boolean = true
    override def deduplicate: Boolean = true
  }

  /**
   * Java API: Retry failed insert requests with best-effort deduplication
   * @see [[https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency BigQuery reference]]
   */
  def withDeduplication = WithDeduplication
}
