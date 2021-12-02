/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.kinesis.KinesisFlowSettings;
import akka.stream.alpakka.kinesis.ShardIterators;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.alpakka.kinesis.javadsl.KinesisFlow;
import akka.stream.alpakka.kinesis.javadsl.KinesisSink;
import akka.stream.alpakka.kinesis.javadsl.KinesisSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
// #init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
// #init-client
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.Record;
// #source-settings
// #source-settings

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class KinesisSnippets {

  public void snippets() {
    // #init-client

    final ActorSystem system = ActorSystem.create();

    final software.amazon.awssdk.services.kinesis.KinesisAsyncClient amazonKinesisAsync =
        KinesisAsyncClient.builder()
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // Possibility to configure the retry policy
            // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
            // .overrideConfiguration(...)
            .build();

    system.registerOnTermination(amazonKinesisAsync::close);
    // #init-client

    // #source-settings
    final ShardSettings settings =
        ShardSettings.create("streamName", "shard-id")
            .withRefreshInterval(Duration.ofSeconds(1))
            .withLimit(500)
            .withShardIterator(ShardIterators.trimHorizon());
    // #source-settings

    // #source-single
    final Source<software.amazon.awssdk.services.kinesis.model.Record, NotUsed> source =
        KinesisSource.basic(settings, amazonKinesisAsync);
    // #source-single

    // #source-list
    final List<ShardSettings> mergeSettings =
        Arrays.asList(
            ShardSettings.create("streamName", "shard-id-1"),
            ShardSettings.create("streamName", "shard-id-2"));
    final Source<Record, NotUsed> two = KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync);
    // #source-list

    // #flow-settings
    final KinesisFlowSettings flowSettings =
        KinesisFlowSettings.create()
            .withParallelism(1)
            .withMaxBatchSize(500)
            .withMaxRecordsPerSecond(1_000)
            .withMaxBytesPerSecond(1_000_000)
            .withMaxRecordsPerSecond(5);

    final KinesisFlowSettings defaultFlowSettings = KinesisFlowSettings.create();

    final KinesisFlowSettings fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4);
    // #flow-settings

    // #flow-sink
    final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> flow =
        KinesisFlow.create("streamName", flowSettings, amazonKinesisAsync);

    final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> defaultSettingsFlow =
        KinesisFlow.create("streamName", amazonKinesisAsync);

    final FlowWithContext<PutRecordsRequestEntry, String, PutRecordsResultEntry, String, NotUsed>
        flowWithStringContext =
            KinesisFlow.createWithContext("streamName", flowSettings, amazonKinesisAsync);

    final FlowWithContext<PutRecordsRequestEntry, String, PutRecordsResultEntry, String, NotUsed>
        defaultSettingsFlowWithStringContext =
            KinesisFlow.createWithContext("streamName", flowSettings, amazonKinesisAsync);

    final Sink<PutRecordsRequestEntry, NotUsed> sink =
        KinesisSink.create("streamName", flowSettings, amazonKinesisAsync);

    final Sink<PutRecordsRequestEntry, NotUsed> defaultSettingsSink =
        KinesisSink.create("streamName", amazonKinesisAsync);
    // #flow-sink

    // #error-handling
    final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> flowWithErrors =
        KinesisFlow.create("streamName", flowSettings, amazonKinesisAsync)
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
