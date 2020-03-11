/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.util.JavaDurationConverters._
import com.datastax.oss.driver.api.core.cql.BatchType

import scala.concurrent.duration.{FiniteDuration, _}

class CassandraWriteSettings private (val parallelism: Int,
                                      val maxBatchSize: Int,
                                      val maxBatchWait: FiniteDuration,
                                      val batchType: BatchType) {
  require(parallelism > 0, s"Invalid value for parallelism: $parallelism. It should be > 0.")
  require(maxBatchSize > 0, s"Invalid value for maxBatchSize: $maxBatchSize. It should be > 0.")

  /**
   * WARNING: setting a write parallelism other than 1 will lead to out-of-order updates
   */
  def withParallelism(value: Int): CassandraWriteSettings = copy(parallelism = value)

  /**
   * Batch size for `CassandraFlow.createUnloggedBatch`.
   */
  def withMaxBatchSize(maxBatchSize: Int): CassandraWriteSettings =
    copy(maxBatchSize = maxBatchSize)

  /**
   * Batch grouping time for `CassandraFlow.createUnloggedBatch`.
   */
  def withMaxBatchWait(maxBatchWait: FiniteDuration): CassandraWriteSettings =
    copy(maxBatchWait = maxBatchWait)

  /**
   * Java API: Batch grouping time for `CassandraFlow.createUnloggedBatch`.
   */
  def withMaxBatchWait(maxBatchWait: java.time.Duration): CassandraWriteSettings =
    copy(maxBatchWait = maxBatchWait.asScala)

  def withBatchType(value: BatchType): CassandraWriteSettings =
    copy(batchType = value)

  private def copy(parallelism: Int = parallelism,
                   maxBatchSize: Int = maxBatchSize,
                   maxBatchWait: FiniteDuration = maxBatchWait,
                   batchType: BatchType = batchType) =
    new CassandraWriteSettings(parallelism, maxBatchSize, maxBatchWait, batchType)

  override def toString: String =
    "CassandraWriteSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxBatchWait=$maxBatchWait," +
    s"batchType=$batchType)"

}

object CassandraWriteSettings {
  val defaults: CassandraWriteSettings = new CassandraWriteSettings(1, 100, 500.millis, BatchType.LOGGED)

  def create(): CassandraWriteSettings = defaults
  def apply(): CassandraWriteSettings = defaults
}
