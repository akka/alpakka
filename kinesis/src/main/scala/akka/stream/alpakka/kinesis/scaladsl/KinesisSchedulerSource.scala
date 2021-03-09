/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream._
import akka.stream.alpakka.kinesis.impl.KinesisSchedulerSourceStage
import akka.stream.alpakka.kinesis.{
  CommittableRecord,
  KinesisSchedulerCheckpointSettings,
  KinesisSchedulerSourceSettings
}
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source, SubFlow}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.collection.immutable
import scala.concurrent.Future

object KinesisSchedulerSource {

  /**
   * Source that spawns a single Kinesis KCL Scheduler capable of consuming records from multiple Kinesis
   * shards in coordination with other KCL Scheduler instances connected to the same stream. Only a single
   * KCL Scheduler instance can own the lease of a shard at any given time. The Kinesis service assigns each
   * shard lease among the available instances.
   *
   * Every message emitted downstream belongs to a shard. Messages can be committed to save the progress of
   * the client consuming that shard.
   */
  def apply(
      schedulerBuilder: ShardRecordProcessorFactory => Scheduler,
      settings: KinesisSchedulerSourceSettings
  ): Source[CommittableRecord, Future[Scheduler]] =
    Source
      .fromMaterializer { (mat, _) =>
        import mat.executionContext
        Source
          .fromGraph(new KinesisSchedulerSourceStage(settings, schedulerBuilder))
      }
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.parasitic))

  def sharded(
      schedulerBuilder: ShardRecordProcessorFactory => Scheduler,
      settings: KinesisSchedulerSourceSettings
  ): SubFlow[CommittableRecord, Future[Scheduler], Source[CommittableRecord, Future[Scheduler]]#Repr, RunnableGraph[
    Future[Scheduler]
  ]] =
    apply(schedulerBuilder, settings)
      .groupBy(MAX_KINESIS_SHARDS, _.processorData.shardId)

  def checkpointRecordsFlow(
      settings: KinesisSchedulerCheckpointSettings
  ): Flow[CommittableRecord, KinesisClientRecord, NotUsed] =
    Flow[CommittableRecord]
      .groupBy(MAX_KINESIS_SHARDS, _.processorData.shardId)
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(checkpointRecordBatch)
      .mergeSubstreams

  private val checkpointRecordBatch =
    Flow[immutable.Seq[CommittableRecord]]
      .map(records => {
        records.max(CommittableRecord.orderBySequenceNumber).tryToCheckpoint()
        records
      })
      .mapConcat(identity)
      .map(_.record)
      .addAttributes(Attributes(ActorAttributes.IODispatcher))

  def checkpointRecordsSink(
      settings: KinesisSchedulerCheckpointSettings
  ): Sink[CommittableRecord, NotUsed] =
    checkpointRecordsFlow(settings).to(Sink.ignore)

  // http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
  private val MAX_KINESIS_SHARDS = 500

}
