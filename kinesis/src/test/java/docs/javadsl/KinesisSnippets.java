/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesis.KinesisFlowSettings;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.alpakka.kinesis.javadsl.KinesisFlow;
import akka.stream.alpakka.kinesis.javadsl.KinesisSink;
import akka.stream.alpakka.kinesis.javadsl.KinesisSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class KinesisSnippets {

  // #init-client
  final ActorSystem system = ActorSystem.create();
  final ActorMaterializer materializer = ActorMaterializer.create(system);

  final software.amazon.awssdk.services.kinesis.KinesisAsyncClient amazonKinesisAsync =
      KinesisAsyncClient.create();
  // #init-client

  {
    // #init-client

    system.registerOnTermination(amazonKinesisAsync::close);
    // #init-client
  }

  // #source-settings
  final ShardSettings settings =
      ShardSettings.create("streamName", "shard-id")
          .withRefreshInterval(Duration.ofSeconds(1))
          .withLimit(500)
          .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
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
          .withMaxRecordsPerSecond(5)
          .withBackoffStrategyExponential()
          .withRetryInitialTimeout(Duration.ofMillis(100));

  final KinesisFlowSettings defaultFlowSettings = KinesisFlowSettings.create();

  final KinesisFlowSettings fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4);
  // #flow-settings

  // #flow-sink
  final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> flow =
      KinesisFlow.create("streamName", flowSettings, amazonKinesisAsync);

  final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> defaultSettingsFlow =
      KinesisFlow.create("streamName", amazonKinesisAsync);

  final Flow<Pair<PutRecordsRequestEntry, String>, Pair<PutRecordsResultEntry, String>, NotUsed>
      flowWithStringContext =
          KinesisFlow.withUserContext("streamName", flowSettings, amazonKinesisAsync);

  final Flow<Pair<PutRecordsRequestEntry, String>, Pair<PutRecordsResultEntry, String>, NotUsed>
      defaultSettingsFlowWithStringContext =
          KinesisFlow.withUserContext("streamName", flowSettings, amazonKinesisAsync);

  final Sink<PutRecordsRequestEntry, NotUsed> sink =
      KinesisSink.create("streamName", flowSettings, amazonKinesisAsync);

  final Sink<PutRecordsRequestEntry, NotUsed> defaultSettingsSink =
      KinesisSink.create("streamName", amazonKinesisAsync);
  // #flow-sink

}
