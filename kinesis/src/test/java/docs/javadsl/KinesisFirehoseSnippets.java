/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings;
import akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseFlow;
import akka.stream.alpakka.kinesisfirehose.javadsl.KinesisFirehoseSink;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
// #init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
// #init-client
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

public class KinesisFirehoseSnippets {

  public void snippets() {
    // #init-client

    final ActorSystem system = ActorSystem.create();

    final software.amazon.awssdk.services.firehose.FirehoseAsyncClient amazonFirehoseAsync =
        FirehoseAsyncClient.builder()
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // Possibility to configure the retry policy
            // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
            // .overrideConfiguration(...)
            .build();

    system.registerOnTermination(amazonFirehoseAsync::close);
    // #init-client

    // #flow-settings
    final KinesisFirehoseFlowSettings flowSettings =
        KinesisFirehoseFlowSettings.create()
            .withParallelism(1)
            .withMaxBatchSize(500)
            .withMaxRecordsPerSecond(1_000)
            .withMaxBytesPerSecond(1_000_000)
            .withMaxRecordsPerSecond(5);

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

    // #error-handling
    final Flow<Record, PutRecordBatchResponseEntry, NotUsed> flowWithErrors =
        KinesisFirehoseFlow.apply("streamName", flowSettings, amazonFirehoseAsync)
            .map(
                response -> {
                  if (response.errorCode() != null) {
                    throw new RuntimeException(response.errorCode());
                  }
                  return response;
                });
    // #error-handling
  }
}
