/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

final class KinesisFlowSettings private (val parallelism: Int,
                                         val maxBatchSize: Int,
                                         val maxRecordsPerSecond: Int,
                                         val maxBytesPerSecond: Int
) {

  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)

  def withParallelism(value: Int): KinesisFlowSettings = copy(parallelism = value)
  def withMaxBatchSize(value: Int): KinesisFlowSettings = copy(maxBatchSize = value)
  def withMaxRecordsPerSecond(value: Int): KinesisFlowSettings = copy(maxRecordsPerSecond = value)
  def withMaxBytesPerSecond(value: Int): KinesisFlowSettings = copy(maxBytesPerSecond = value)

  private def copy(
      parallelism: Int = parallelism,
      maxBatchSize: Int = maxBatchSize,
      maxRecordsPerSecond: Int = maxRecordsPerSecond,
      maxBytesPerSecond: Int = maxBytesPerSecond
  ): KinesisFlowSettings = new KinesisFlowSettings(
    parallelism = parallelism,
    maxBatchSize = maxBatchSize,
    maxRecordsPerSecond = maxRecordsPerSecond,
    maxBytesPerSecond = maxBytesPerSecond
  )

  override def toString =
    "KinesisFlowSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxRecordsPerSecond=$maxRecordsPerSecond," +
    s"maxBytesPerSecond=$maxBytesPerSecond" +
    ")"
}

object KinesisFlowSettings {
  private val MAX_RECORDS_PER_REQUEST = 500
  private val MAX_RECORDS_PER_SHARD_PER_SECOND = 1000
  private val MAX_BYTES_PER_SHARD_PER_SECOND = 1000000

  val Defaults: KinesisFlowSettings = byNumberOfShards(1)

  def apply(): KinesisFlowSettings = Defaults

  def byNumberOfShards(shards: Int): KinesisFlowSettings =
    new KinesisFlowSettings(
      parallelism = shards * (MAX_RECORDS_PER_SHARD_PER_SECOND / MAX_RECORDS_PER_REQUEST),
      maxBatchSize = MAX_RECORDS_PER_REQUEST,
      maxRecordsPerSecond = shards * MAX_RECORDS_PER_SHARD_PER_SECOND,
      maxBytesPerSecond = shards * MAX_BYTES_PER_SHARD_PER_SECOND
    )

  /** Java API */
  def create(): KinesisFlowSettings = Defaults

}
