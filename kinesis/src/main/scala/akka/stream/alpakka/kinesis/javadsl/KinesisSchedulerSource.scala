/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.alpakka.kinesis.{CommittableRecord, scaladsl, _}
import akka.stream.javadsl.{Flow, Sink, Source, SubSource}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

object KinesisSchedulerSource {

  abstract class SchedulerBuilder {
    def build(r: ShardRecordProcessorFactory): Scheduler
  }

  def create(
      schedulerBuilder: SchedulerBuilder,
      settings: KinesisSchedulerSourceSettings
  ): Source[CommittableRecord, CompletionStage[Scheduler]] =
    scaladsl.KinesisSchedulerSource
      .apply(schedulerBuilder.build, settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  def createSharded(
      schedulerBuilder: SchedulerBuilder,
      settings: KinesisSchedulerSourceSettings
  ): SubSource[CommittableRecord, Future[Scheduler]] =
    new SubSource(
      scaladsl.KinesisSchedulerSource
        .sharded(schedulerBuilder.build, settings)
    )

  def checkpointRecordsFlow(
      settings: KinesisSchedulerCheckpointSettings
  ): Flow[CommittableRecord, KinesisClientRecord, NotUsed] =
    scaladsl.KinesisSchedulerSource
      .checkpointRecordsFlow(settings)
      .asJava

  def checkpointRecordsSink(
      settings: KinesisSchedulerCheckpointSettings
  ): Sink[CommittableRecord, NotUsed] =
    scaladsl.KinesisSchedulerSource
      .checkpointRecordsSink(settings)
      .asJava

}
