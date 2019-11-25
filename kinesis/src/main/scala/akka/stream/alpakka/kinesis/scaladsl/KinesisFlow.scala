/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.alpakka.kinesis.impl.KinesisFlowStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

object KinesisFlow {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .map((_, ()))
      .via(withUserContext(streamName, settings))
      .map(_._1)

  def withUserContext[T](streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[(PutRecordsRequestEntry, T), (PutRecordsResultEntry, T), NotUsed] =
    Flow[(PutRecordsRequestEntry, T)]
      .throttle(settings.maxRecordsPerSecond, 1.second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
      .throttle(settings.maxBytesPerSecond,
                1.second,
                settings.maxBytesPerSecond,
                getPayloadByteSize,
                ThrottleMode.Shaping)
      .batch(settings.maxBatchSize, Queue(_))(_ :+ _)
      .via(
        new KinesisFlowStage(
          streamName
        )
      )
      .mapAsync(settings.parallelism)(identity)
      .mapConcat(identity)

  private def getPayloadByteSize[T](record: (PutRecordsRequestEntry, T)): Int = record match {
    case (request, _) => request.partitionKey.length + request.data.asByteBuffer.position()
  }

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[(String, ByteBuffer), PutRecordsResultEntry, NotUsed] =
    Flow[(String, ByteBuffer)]
      .map {
        case (partitionKey, data) =>
          PutRecordsRequestEntry
            .builder()
            .partitionKey(partitionKey)
            .data(SdkBytes.fromByteBuffer(data))
            .build()
      }
      .via(apply(streamName, settings))

  def byPartitionAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[(String, ByteString), PutRecordsResultEntry, NotUsed] =
    Flow[(String, ByteString)]
      .map {
        case (partitionKey, bytes) =>
          partitionKey -> bytes.toByteBuffer
      }
      .via(byPartitionAndData(streamName, settings))

}
