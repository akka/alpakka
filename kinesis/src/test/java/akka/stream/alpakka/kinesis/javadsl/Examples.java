/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kinesis.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.kinesis.ShardSettings;
import akka.stream.javadsl.Source;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
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

    //#settings
    final ShardSettings settings = ShardSettings.create("streamName", "shard-id", ShardIteratorType.LATEST, FiniteDuration.apply(1L, TimeUnit.SECONDS), 500);
    //#settings

    //#single
    final Source<Record, NotUsed> single = KinesisSource.basic(settings, amazonKinesisAsync);
    //#single

    //#list
    final Source<Record, NotUsed> two = KinesisSource.basicMerge(Arrays.asList(settings), amazonKinesisAsync);
    //#list

}
