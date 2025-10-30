/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.scaladsl.KinesisSchedulerSource
import akka.stream.alpakka.kinesis.{KinesisSchedulerCheckpointSettings, KinesisSchedulerSourceSettings}
import akka.stream.scaladsl.Sink
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.ShardRecordProcessorFactory

import scala.concurrent.duration._

class KclSnippets {

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  //#init-system

  //#init-clients
  val region: Region = Region.EU_WEST_1
  val kinesisClient: KinesisAsyncClient = KinesisAsyncClient.builder.region(region).build
  val dynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder.region(region).build
  val cloudWatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder.region(region).build
  //#init-clients

  //#scheduler-settings
  val schedulerSourceSettings = KinesisSchedulerSourceSettings(bufferSize = 1000, backpressureTimeout = 1.minute)

  val builder: ShardRecordProcessorFactory => Scheduler =
    recordProcessorFactory => {

      val streamName = "myStreamName"

      val configsBuilder = new ConfigsBuilder(
        streamName,
        "myApp",
        kinesisClient,
        dynamoClient,
        cloudWatchClient,
        s"${
          import scala.sys.process._
          "hostname".!!.trim()
        }:${UUID.randomUUID()}",
        recordProcessorFactory
      )

      new Scheduler(
        configsBuilder.checkpointConfig,
        configsBuilder.coordinatorConfig,
        configsBuilder.leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        configsBuilder.metricsConfig,
        configsBuilder.processorConfig,
        configsBuilder.retrievalConfig
      )
    }
  //#scheduler-settings

  //#scheduler-source
  val source = KinesisSchedulerSource(builder, schedulerSourceSettings)
    .log("kinesis-records", "Consumed record " + _.sequenceNumber)
  //#scheduler-source

  //#checkpoint
  val checkpointSettings = KinesisSchedulerCheckpointSettings(100, 30.seconds)

  source
    .via(KinesisSchedulerSource.checkpointRecordsFlow(checkpointSettings))
    .to(Sink.ignore)
  source
    .to(KinesisSchedulerSource.checkpointRecordsSink(checkpointSettings))
  //#checkpoint

}
