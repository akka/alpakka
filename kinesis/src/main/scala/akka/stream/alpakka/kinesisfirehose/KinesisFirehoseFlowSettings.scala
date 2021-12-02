/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

final class KinesisFirehoseFlowSettings private (val parallelism: Int,
                                                 val maxBatchSize: Int,
                                                 val maxRecordsPerSecond: Int,
                                                 val maxBytesPerSecond: Int) {

  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)

  def withParallelism(value: Int): KinesisFirehoseFlowSettings = copy(parallelism = value)
  def withMaxBatchSize(value: Int): KinesisFirehoseFlowSettings = copy(maxBatchSize = value)
  def withMaxRecordsPerSecond(value: Int): KinesisFirehoseFlowSettings = copy(maxRecordsPerSecond = value)
  def withMaxBytesPerSecond(value: Int): KinesisFirehoseFlowSettings = copy(maxBytesPerSecond = value)

  private def copy(
      parallelism: Int = parallelism,
      maxBatchSize: Int = maxBatchSize,
      maxRecordsPerSecond: Int = maxRecordsPerSecond,
      maxBytesPerSecond: Int = maxBytesPerSecond
  ): KinesisFirehoseFlowSettings = new KinesisFirehoseFlowSettings(
    parallelism = parallelism,
    maxBatchSize = maxBatchSize,
    maxRecordsPerSecond = maxRecordsPerSecond,
    maxBytesPerSecond = maxBytesPerSecond
  )

  override def toString =
    "KinesisFirehoseFlowSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxRecordsPerSecond=$maxRecordsPerSecond," +
    s"maxBytesPerSecond=$maxBytesPerSecond" +
    ")"
}

object KinesisFirehoseFlowSettings {
  private val MaxRecordsPerRequest = 500
  private val MaxRecordsPerSecond = 5000
  private val MaxBytesPerSecond = 4000000

  val Defaults: KinesisFirehoseFlowSettings = new KinesisFirehoseFlowSettings(
    parallelism = MaxRecordsPerSecond / MaxRecordsPerRequest,
    maxBatchSize = MaxRecordsPerRequest,
    maxRecordsPerSecond = MaxRecordsPerSecond,
    maxBytesPerSecond = MaxBytesPerSecond
  )

  /** Scala API */
  def apply(): KinesisFirehoseFlowSettings = Defaults

  /** Java API */
  def create(): KinesisFirehoseFlowSettings = Defaults
}
