/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer
import java.util.Date

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, ShardSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry, Record, ShardIteratorType}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.concurrent.duration._

object Examples {

  //#init-client
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()

  implicit val amazonKinesisAsync: com.amazonaws.services.kinesis.AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.defaultClient()

  system.registerOnTermination(amazonKinesisAsync.shutdown())
  //#init-client

  //#source-settings
  val settings = ShardSettings(streamName = "myStreamName",
                               shardId = "shard-id",
                               shardIteratorType = ShardIteratorType.TRIM_HORIZON,
                               refreshInterval = 1.second,
                               limit = 500)
  //#source-settings

  //#source-single
  val source: Source[com.amazonaws.services.kinesis.model.Record, NotUsed] =
    KinesisSource.basic(settings, amazonKinesisAsync)
  //#source-single

  //#source-list
  val mergeSettings = List(
    ShardSettings("myStreamName",
                  "shard-id-1",
                  ShardIteratorType.AT_SEQUENCE_NUMBER,
                  startingSequenceNumber = Some("sequence"),
                  refreshInterval = 1.second,
                  limit = 500),
    ShardSettings("myStreamName",
                  "shard-id-2",
                  ShardIteratorType.AT_TIMESTAMP,
                  atTimestamp = Some(new Date()),
                  refreshInterval = 1.second,
                  limit = 500)
  )

  val mergedSource: Source[Record, NotUsed] = KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync)
  //#source-list

  //#flow-settings
  val flowSettings = KinesisFlowSettings(
    parallelism = 1,
    maxBatchSize = 500,
    maxRecordsPerSecond = 1000,
    maxBytesPerSecond = 1000000,
    maxRetries = 5,
    backoffStrategy = KinesisFlowSettings.Exponential,
    retryInitialTimeout = 100.millis
  )

  val defaultFlowSettings = KinesisFlowSettings.defaultInstance

  val fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4)
  //#flow-settings

  //#flow-sink
  val flow1: Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = KinesisFlow("myStreamName")

  val flow2: Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = KinesisFlow("myStreamName", flowSettings)

  val flow3: Flow[(String, ByteString), PutRecordsResultEntry, NotUsed] =
    KinesisFlow.byPartitionAndBytes("myStreamName")

  val flow4: Flow[(String, ByteBuffer), PutRecordsResultEntry, NotUsed] =
    KinesisFlow.byPartitionAndData("myStreamName")

  val sink1: Sink[PutRecordsRequestEntry, NotUsed] = KinesisSink("myStreamName")
  val sink2: Sink[PutRecordsRequestEntry, NotUsed] = KinesisSink("myStreamName", flowSettings)
  val sink3: Sink[(String, ByteString), NotUsed] = KinesisSink.byPartitionAndBytes("myStreamName")
  val sink4: Sink[(String, ByteBuffer), NotUsed] = KinesisSink.byPartitionAndData("myStreamName")
  //#flow-sink

}
