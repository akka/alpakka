/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer
import java.util.Date

import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, ShardSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, ShardIteratorType}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.concurrent.duration._
import scala.language.postfixOps

object Examples {

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-system

  //#init-client
  val amazonKinesisAsync: AmazonKinesisAsync = AmazonKinesisAsyncClientBuilder.defaultClient()
  //#init-client

  //#source-settings
  val settings = ShardSettings(streamName = "myStreamName",
                               shardId = "shard-id",
                               shardIteratorType = ShardIteratorType.TRIM_HORIZON,
                               refreshInterval = 1.second,
                               limit = 500)
  //#source-settings

  //#source-single
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
  KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync)
  //#source-list

  //#flow-settings
  val flowSettings = KinesisFlowSettings(
    parallelism = 1,
    maxBatchSize = 500,
    maxRecordsPerSecond = 1000,
    maxBytesPerSecond = 1000000,
    maxRetries = 5,
    backoffStrategy = KinesisFlowSettings.Exponential,
    retryInitialTimeout = 100 millis
  )

  val defaultFlowSettings = KinesisFlowSettings.defaultInstance

  val fourShardFlowSettings = KinesisFlowSettings.byNumberOfShards(4)
  //#flow-settings

  //#flow-sink
  implicit val _: AmazonKinesisAsync = amazonKinesisAsync

  Source.empty[PutRecordsRequestEntry].via(KinesisFlow("myStreamName")).to(Sink.ignore)
  Source.empty[PutRecordsRequestEntry].via(KinesisFlow("myStreamName", flowSettings)).to(Sink.ignore)
  Source.empty[(String, ByteString)].via(KinesisFlow.byParititonAndBytes("myStreamName")).to(Sink.ignore)
  Source.empty[(String, ByteBuffer)].via(KinesisFlow.byPartitionAndData("myStreamName")).to(Sink.ignore)

  Source.empty[PutRecordsRequestEntry].to(KinesisSink("myStreamName"))
  Source.empty[PutRecordsRequestEntry].to(KinesisSink("myStreamName", flowSettings))
  Source.empty[(String, ByteString)].to(KinesisSink.byParititonAndBytes("myStreamName"))
  Source.empty[(String, ByteBuffer)].to(KinesisSink.byPartitionAndData("myStreamName"))
  //#flow-sink

}
