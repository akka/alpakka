/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, KinesisFlowStage}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}

import scala.collection.immutable.Queue
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._
import scala.language.postfixOps

object KinesisFlow {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance)(
      implicit kinesisClient: AmazonKinesisAsync
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .throttle(settings.maxRecordsPerSecond, 1 second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
      .throttle(settings.maxBytesPerSecond, 1 second, settings.maxBytesPerSecond, getByteSize, ThrottleMode.Shaping)
      .batch(settings.maxBatchSize, Queue(_))(_ :+ _)
      .via(
        new KinesisFlowStage(
          streamName,
          settings.maxRetries,
          settings.backoffStrategy,
          settings.retryInitialTimeout
        )
      )
      .mapAsync(settings.parallelism)(identity)
      .mapConcat(_.getRecords.asScala.to[immutable.Iterable])
      .filter(_.getErrorCode == null)

  private def getByteSize(record: PutRecordsRequestEntry): Int =
    record.getPartitionKey.length + record.getData.position

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Flow[(String, ByteBuffer), PutRecordsResultEntry, NotUsed] =
    Flow[(String, ByteBuffer)]
      .map {
        case (partitionKey, data) =>
          new PutRecordsRequestEntry()
            .withPartitionKey(partitionKey)
            .withData(data)
      }
      .via(apply(streamName, settings))

  def byParititonAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Flow[(String, ByteString), PutRecordsResultEntry, NotUsed] =
    Flow[(String, ByteString)]
      .map {
        case (partitionKey, bytes) =>
          partitionKey -> bytes.toByteBuffer
      }
      .via(byPartitionAndData(streamName, settings))

}
