/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer
import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts.parasitic
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
import scala.util.{Failure, Success, Try}

object KinesisFlow {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    Flow[PutRecordsRequestEntry]
      .map((_, ()))
      .via(withContext(streamName, settings))
      .map(_._1)

  /**
   * Creates default implementation of `KinesisFlow` that takes in `PutRecordsRequestEntry` with context and emits
   * `PutRecordsResultEntry` with context.
   *
   * To note is that the flow first does batching according to the `KinesisFlowSettings` provided and then
   * writes the data in batches via the `KinesisAsyncClient`. On any error from the client, the flow will fail.
   *
   * If it is necessary to have special handling for batching or of errors and successful results
   * the methods @see [[KinesisFlow.batchingFlow]] & @see [[KinesisFlow.batchWritingFlow]] can be used
   * and combined in other ways than the default in this method.
   */
  def withContext[T](streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): FlowWithContext[PutRecordsRequestEntry, T, PutRecordsResultEntry, T, NotUsed] =
    FlowWithContext.fromTuples(
      batchingFlow(settings)
        .via(
          batchWritingFlow[PutRecordsResultEntry, T](
            streamName,
            batch => {
              case Success(putRecordsResponse) => Success(handlePutRecordsSuccess(batch)(putRecordsResponse))
              case Failure(throwable) => Failure(FailurePublishingRecords(throwable))
            },
            settings: KinesisFlowSettings
          )
        )
    )

  def batchingFlow[T](
      settings: KinesisFlowSettings
  ): Flow[(PutRecordsRequestEntry, T), Iterable[(PutRecordsRequestEntry, T)], NotUsed] =
    Flow[(PutRecordsRequestEntry, T)]
      .throttle(settings.maxRecordsPerSecond, 1.second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
      .throttle(settings.maxBytesPerSecond,
                1.second,
                settings.maxBytesPerSecond,
                getPayloadByteSize,
                ThrottleMode.Shaping)
      .batch(settings.maxBatchSize, Queue(_))(_ :+ _)

  def batchWritingFlow[S, T](
      streamName: String,
      handleBatch: Iterable[(PutRecordsRequestEntry, T)] => Try[PutRecordsResponse] => Try[Iterable[(S, T)]],
      settings: KinesisFlowSettings
  )(
      implicit kinesisClient: KinesisAsyncClient
  ): Flow[Iterable[(PutRecordsRequestEntry, T)], (S, T), NotUsed] = {
    checkClient(kinesisClient)
    Flow[Iterable[(PutRecordsRequestEntry, T)]]
      .mapAsync(settings.parallelism)(
        entries =>
          kinesisClient
            .putRecords(
              PutRecordsRequest.builder().streamName(streamName).records(entries.map(_._1).asJavaCollection).build
            )
            .toScala
            .transform(handleBatch(entries))(parasitic)
      )
      .mapConcat(identity)
  }

  def handlePutRecordsSuccess[T](
      entries: Iterable[(PutRecordsRequestEntry, T)]
  )(result: PutRecordsResponse): List[(PutRecordsResultEntry, T)] =
    result.records.asScala.toList.zip(entries).map { case (res, (_, t)) => (res, t) }

  private[kinesis] def getPayloadByteSize[T](record: (PutRecordsRequestEntry, T)): Int = record match {
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

  @InternalApi
  private[scaladsl] def checkClient(kinesisClient: KinesisAsyncClient): Unit =
    require(kinesisClient != null, "The `KinesisAsyncClient` passed in may not be null.")

}
