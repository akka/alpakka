/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.kinesis.CommittableRecord;
import akka.stream.alpakka.kinesis.KinesisSchedulerCheckpointSettings;
import akka.stream.alpakka.kinesis.KinesisSchedulerSourceSettings;
import akka.stream.alpakka.kinesis.javadsl.KinesisSchedulerSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionStage;

public class KclSnippets {

  // #scheduler-settings
  final KinesisSchedulerSource.SchedulerBuilder schedulerBuilder =
      new KinesisSchedulerSource.SchedulerBuilder() {
        @Override
        public Scheduler build(ShardRecordProcessorFactory r) {
          return null; // build your own Scheduler here
        }
      };
  final KinesisSchedulerSourceSettings schedulerSettings =
      KinesisSchedulerSourceSettings.create(1000, Duration.of(1L, ChronoUnit.SECONDS));
  // #scheduler-settings

  // #scheduler-source
  final Source<CommittableRecord, CompletionStage<Scheduler>> schedulerSource =
      KinesisSchedulerSource.create(schedulerBuilder, schedulerSettings);
  // #scheduler-source

  // #checkpoint
  final KinesisSchedulerCheckpointSettings checkpointSettings =
      KinesisSchedulerCheckpointSettings.create(1000, Duration.of(30L, ChronoUnit.SECONDS));
  final Flow<CommittableRecord, KinesisClientRecord, NotUsed> checkpointFlow =
      KinesisSchedulerSource.checkpointRecordsFlow(checkpointSettings);
  // #checkpoint

}
