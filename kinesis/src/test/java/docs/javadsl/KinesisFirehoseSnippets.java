/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings;
import akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseFlow;
import akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseSink;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.time.Duration;

public class KinesisFirehoseSnippets {

  // #init-client
  final ActorSystem system = ActorSystem.create();
  final ActorMaterializer materializer = ActorMaterializer.create(system);

  final software.amazon.awssdk.services.firehose.FirehoseAsyncClient amazonFirehoseAsync =
      FirehoseAsyncClient.create();
  // #init-client

  {
    // #init-client

    system.registerOnTermination(amazonFirehoseAsync::close);
    // #init-client
  }

  // #flow-settings
  final KinesisFirehoseFlowSettings flowSettings =
      KinesisFirehoseFlowSettings.create()
          .withParallelism(1)
          .withMaxBatchSize(500)
          .withMaxRecordsPerSecond(1_000)
          .withMaxBytesPerSecond(1_000_000)
          .withMaxRecordsPerSecond(5)
          .withBackoffStrategyExponential()
          .withRetryInitialTimeout(Duration.ofMillis(100L));

  final KinesisFirehoseFlowSettings defaultFlowSettings = KinesisFirehoseFlowSettings.create();
  // #flow-settings

  // #flow-sink
  final Flow<Record, PutRecordBatchResponseEntry, NotUsed> flow =
      KinesisFirehoseFlow.apply("streamName", flowSettings, amazonFirehoseAsync);

  final Flow<Record, PutRecordBatchResponseEntry, NotUsed> defaultSettingsFlow =
      KinesisFirehoseFlow.apply("streamName", amazonFirehoseAsync);

  final Sink<Record, NotUsed> sink =
      KinesisFirehoseSink.apply("streamName", flowSettings, amazonFirehoseAsync);

  final Sink<Record, NotUsed> defaultSettingsSink =
      KinesisFirehoseSink.apply("streamName", amazonFirehoseAsync);
  // #flow-sink

}
