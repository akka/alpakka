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
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.Record;

import java.time.Duration;

public class KinesisFirehoseSnippets {

  // #init-client
  final ActorSystem system = ActorSystem.create();
  final ActorMaterializer materializer = ActorMaterializer.create(system);

  final com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
      amazonKinesisFirehoseAsync = AmazonKinesisFirehoseAsyncClientBuilder.defaultClient();
  // #init-client

  {
    // #init-client

    system.registerOnTermination(amazonKinesisFirehoseAsync::shutdown);
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
      KinesisFirehoseFlow.apply("streamName", flowSettings, amazonKinesisFirehoseAsync);

  final Flow<Record, PutRecordBatchResponseEntry, NotUsed> defaultSettingsFlow =
      KinesisFirehoseFlow.apply("streamName", amazonKinesisFirehoseAsync);

  final Sink<Record, NotUsed> sink =
      KinesisFirehoseSink.apply("streamName", flowSettings, amazonKinesisFirehoseAsync);

  final Sink<Record, NotUsed> defaultSettingsSink =
      KinesisFirehoseSink.apply("streamName", amazonKinesisFirehoseAsync);
  // #flow-sink

}
