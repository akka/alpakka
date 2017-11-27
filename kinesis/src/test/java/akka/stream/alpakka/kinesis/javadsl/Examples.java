/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
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

    //#init-client
    final AmazonKinesisAsync amazonKinesisAsync = AmazonKinesisAsyncClientBuilder.defaultClient();
    //#init-client

    //#init-system
    final ActorSystem system = ActorSystem.create();
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    //#init-system

    //#source-settings
    final ShardSettings settings = ShardSettings.create("streamName", "shard-id", ShardIteratorType.LATEST, FiniteDuration.apply(1L, TimeUnit.SECONDS), 500);
    //#source-settings

    //#source-single
    final Source<Record, NotUsed> single = KinesisSource.basic(settings, amazonKinesisAsync);
    //#source-single

    //#source-list
    final Source<Record, NotUsed> two = KinesisSource.basicMerge(Arrays.asList(settings), amazonKinesisAsync);
    //#source-list

    //#flow-settings
    final KinesisFlowSettings flowSettings = KinesisFlowSettings.apply(1,500,1000,1000000,5, KinesisFlowSettings.exponential(), FiniteDuration.apply(100, TimeUnit.MILLISECONDS));

    final KinesisFlowSettings defaultFlowSettings = KinesisFlowSettings.defaultInstance();

    final KinesisFlowSettings fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4);
    //#flow-settings

    //#flow-sink
    final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> flow = KinesisFlow.apply("streamName", flowSettings, amazonKinesisAsync);
    final Flow<PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed> defaultSettingsFlow = KinesisFlow.apply("streamName", amazonKinesisAsync);

    final Sink<PutRecordsRequestEntry, NotUsed> sink = KinesisSink.apply("streamName", flowSettings, amazonKinesisAsync);
    final Sink<PutRecordsRequestEntry, NotUsed> defaultSettingsSink = KinesisSink.apply("streamName", amazonKinesisAsync);
    //#flow-sink

}
