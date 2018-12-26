/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesis.KinesisFlowSettings;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Examples {

  // #init-client
  final ActorSystem system = ActorSystem.create();
  final ActorMaterializer materializer = ActorMaterializer.create(system);

  final com.amazonaws.services.kinesis.AmazonKinesisAsync amazonKinesisAsync =
      AmazonKinesisAsyncClientBuilder.defaultClient();
  // #init-client

  {
    // #init-client

    system.registerOnTermination(amazonKinesisAsync::shutdown);
    // #init-client
  }

  // #source-settings
  final ShardSettings settings =
      ShardSettings.create("streamName", "shard-id")
          .withShardIteratorType(ShardIteratorType.LATEST)
          .withRefreshInterval(1L, TimeUnit.SECONDS)
          .withLimit(500);
  // #source-settings

  // #source-single
  final Source<com.amazonaws.services.kinesis.model.Record, NotUsed> source =
      KinesisSource.basic(settings, amazonKinesisAsync);
  // #source-single

  // #source-list
  final Source<Record, NotUsed> two =
      KinesisSource.basicMerge(Arrays.asList(settings), amazonKinesisAsync);
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
          .withRetryInitialTimeout(100L, TimeUnit.MILLISECONDS);

  final KinesisFlowSettings defaultFlowSettings = KinesisFlowSettings.create();

  final KinesisFlowSettings fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4);
  // #flow-settings

  // #flow-sink
  final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> flow =
      KinesisFlow.apply("streamName", flowSettings, amazonKinesisAsync);

  final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> defaultSettingsFlow =
      KinesisFlow.apply("streamName", amazonKinesisAsync);

  final Flow<Pair<PutRecordsRequestEntry, String>, Pair<PutRecordsResultEntry, String>, NotUsed>
      flowWithStringContext =
          KinesisFlow.withUserContext("streamName", flowSettings, amazonKinesisAsync);

  final Flow<Pair<PutRecordsRequestEntry, String>, Pair<PutRecordsResultEntry, String>, NotUsed>
      defaultSettingsFlowWithStringContext =
          KinesisFlow.withUserContext("streamName", flowSettings, amazonKinesisAsync);

  final Sink<PutRecordsRequestEntry, NotUsed> sink =
      KinesisSink.apply("streamName", flowSettings, amazonKinesisAsync);

  final Sink<PutRecordsRequestEntry, NotUsed> defaultSettingsSink =
      KinesisSink.apply("streamName", amazonKinesisAsync);
  // #flow-sink

}
