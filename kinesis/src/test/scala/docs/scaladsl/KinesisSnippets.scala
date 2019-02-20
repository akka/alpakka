/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.scaladsl.{KinesisFlow, KinesisSink, KinesisSource}
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, ShardSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry, Record, ShardIteratorType}

import scala.concurrent.duration._

object KinesisSnippets {

  //#init-client
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()

  implicit val amazonKinesisAsync: com.amazonaws.services.kinesis.AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.defaultClient()

  system.registerOnTermination(amazonKinesisAsync.shutdown())
  //#init-client

  //#source-settings
  val settings =
    ShardSettings(streamName = "myStreamName", shardId = "shard-id")
      .withRefreshInterval(1.second)
      .withLimit(500)
      .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
  //#source-settings

  //#source-single
  val source: Source[com.amazonaws.services.kinesis.model.Record, NotUsed] =
    KinesisSource.basic(settings, amazonKinesisAsync)
  //#source-single

  //#source-list
  val mergeSettings = List(
    ShardSettings("myStreamName", "shard-id-1"),
    ShardSettings("myStreamName", "shard-id-2")
  )
  val mergedSource: Source[Record, NotUsed] = KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync)
  //#source-list

  //#flow-settings
  val flowSettings = KinesisFlowSettings
    .create()
    .withParallelism(1)
    .withMaxBatchSize(500)
    .withMaxRecordsPerSecond(1000)
    .withMaxBytesPerSecond(1000000)
    .withMaxRetries(5)
    .withBackoffStrategy(KinesisFlowSettings.Exponential)
    .withRetryInitialTimeout(100.milli)

  val defaultFlowSettings = KinesisFlowSettings.Defaults

  val fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4)
  //#flow-settings

  //#flow-sink
  val flow1: Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = KinesisFlow("myStreamName")

  val flow2: Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = KinesisFlow("myStreamName", flowSettings)

  val flow3: Flow[(PutRecordsRequestEntry, String), (PutRecordsResultEntry, String), NotUsed] =
    KinesisFlow.withUserContext("myStreamName")

  val flow4: Flow[(PutRecordsRequestEntry, String), (PutRecordsResultEntry, String), NotUsed] =
    KinesisFlow.withUserContext("myStreamName", flowSettings)

  val flow5: Flow[(String, ByteString), PutRecordsResultEntry, NotUsed] =
    KinesisFlow.byPartitionAndBytes("myStreamName")

  val flow6: Flow[(String, ByteBuffer), PutRecordsResultEntry, NotUsed] =
    KinesisFlow.byPartitionAndData("myStreamName")

  val sink1: Sink[PutRecordsRequestEntry, NotUsed] = KinesisSink("myStreamName")
  val sink2: Sink[PutRecordsRequestEntry, NotUsed] = KinesisSink("myStreamName", flowSettings)
  val sink3: Sink[(String, ByteString), NotUsed] = KinesisSink.byPartitionAndBytes("myStreamName")
  val sink4: Sink[(String, ByteBuffer), NotUsed] = KinesisSink.byPartitionAndData("myStreamName")
  //#flow-sink

}
