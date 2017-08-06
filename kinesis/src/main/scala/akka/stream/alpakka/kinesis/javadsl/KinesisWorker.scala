/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import java.util.concurrent.Executor

import akka.NotUsed
import akka.stream.alpakka.kinesis.worker.CommittableRecord
import akka.stream.alpakka.kinesis.{scaladsl, _}
import akka.stream.javadsl.{Flow, Sink, Source}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.model.Record

import scala.concurrent.ExecutionContext

object KinesisWorker {

  abstract class WorkerBuilder {
    def build(r: IRecordProcessorFactory): Worker
  }

  def create(
      workerBuilder: WorkerBuilder,
      settings: KinesisWorkerSourceSettings,
      workerExecutor: Executor
  ): Source[CommittableRecord, NotUsed] =
    scaladsl.KinesisWorker
      .apply(workerBuilder.build, settings)(ExecutionContext.fromExecutor(workerExecutor))
      .asJava

  def create(
      workerBuilder: WorkerBuilder,
      workerExecutor: Executor
  ): Source[CommittableRecord, NotUsed] =
    create(workerBuilder, KinesisWorkerSourceSettings.defaultInstance, workerExecutor)

  def checkpointRecordsFlow(
      settings: KinesisWorkerCheckpointSettings
  ): Flow[CommittableRecord, Record, NotUsed] =
    scaladsl.KinesisWorker.checkpointRecordsFlow(settings).asJava

  def checkpointRecordsFlow(): Flow[CommittableRecord, Record, NotUsed] =
    checkpointRecordsFlow(KinesisWorkerCheckpointSettings.defaultInstance)

  def checkpointRecordsSink(
      settings: KinesisWorkerCheckpointSettings
  ): Sink[CommittableRecord, NotUsed] =
    scaladsl.KinesisWorker.checkpointRecordsSink(settings).asJava

  def checkpointRecordsSink(): Sink[CommittableRecord, NotUsed] =
    checkpointRecordsSink(KinesisWorkerCheckpointSettings.defaultInstance)
}
