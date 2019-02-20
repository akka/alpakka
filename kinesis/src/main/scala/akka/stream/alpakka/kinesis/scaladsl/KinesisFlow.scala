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
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

object KinesisFlow {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: AmazonKinesisAsync
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .map((_, ()))
      .via(withUserContext(streamName, settings))
      .map(_._1)

  def withUserContext[T](streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: AmazonKinesisAsync
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
          streamName,
          settings.maxRetries,
          settings.backoffStrategy,
          settings.retryInitialTimeout
        )
      )
      .mapAsync(settings.parallelism)(identity)
      .mapConcat(identity)

  private def getPayloadByteSize[T](record: (PutRecordsRequestEntry, T)): Int = record match {
    case (request, _) => request.getPartitionKey.length + request.getData.position()
  }

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
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

  def byPartitionAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
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
