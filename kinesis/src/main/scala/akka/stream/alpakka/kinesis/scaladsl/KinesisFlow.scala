/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.ThrottleMode
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.alpakka.kinesis.KinesisErrors.FailurePublishingRecords
import akka.stream.scaladsl.{Flow, FlowWithContext}
import akka.util.ByteString
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResponse,
  PutRecordsResultEntry
}

import scala.collection.JavaConverters._
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.compat.java8.FutureConverters._

object KinesisFlow {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .map((_, ()))
      .via(withContext(streamName, settings))
      .map(_._1)

  def withContext[T](streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): FlowWithContext[PutRecordsRequestEntry, T, PutRecordsResultEntry, T, NotUsed] =
    FlowWithContext.fromTuples(
      Flow[(PutRecordsRequestEntry, T)]
        .throttle(settings.maxRecordsPerSecond, 1.second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
        .throttle(settings.maxBytesPerSecond,
                  1.second,
                  settings.maxBytesPerSecond,
                  getPayloadByteSize,
                  ThrottleMode.Shaping)
        .batch(settings.maxBatchSize, Queue(_))(_ :+ _)
        .mapAsync(settings.parallelism)(
          entries =>
            kinesisClient
              .putRecords(
                PutRecordsRequest.builder().streamName(streamName).records(entries.map(_._1).asJavaCollection).build
              )
              .toScala
              .transform(handlePutRecordsSuccess(entries), FailurePublishingRecords(_))(sameThreadExecutionContext)
        )
        .mapConcat(identity)
    )

  private def handlePutRecordsSuccess[T](
      entries: Iterable[(PutRecordsRequestEntry, T)]
  )(result: PutRecordsResponse): List[(PutRecordsResultEntry, T)] =
    result.records.asScala.toList.zip(entries).map { case (res, (_, t)) => (res, t) }

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
