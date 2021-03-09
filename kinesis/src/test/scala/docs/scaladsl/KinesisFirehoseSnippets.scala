/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings
import akka.stream.alpakka.kinesisfirehose.scaladsl.{KinesisFirehoseFlow, KinesisFirehoseSink}
import akka.stream.scaladsl.{Flow, Sink}
import software.amazon.awssdk.services.firehose.model.{PutRecordBatchResponseEntry, Record}

object KinesisFirehoseSnippets {

  //#init-client
  import com.github.matsluni.akkahttpspi.AkkaHttpClient
  import software.amazon.awssdk.services.firehose.FirehoseAsyncClient

  implicit val system: ActorSystem = ActorSystem()

  implicit val amazonKinesisFirehoseAsync: software.amazon.awssdk.services.firehose.FirehoseAsyncClient =
    FirehoseAsyncClient
      .builder()
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      // Possibility to configure the retry policy
      // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
      // .overrideConfiguration(...)
      .build()

  system.registerOnTermination(amazonKinesisFirehoseAsync.close())
  //#init-client

  //#flow-settings
  val flowSettings = KinesisFirehoseFlowSettings
    .create()
    .withParallelism(1)
    .withMaxBatchSize(500)
    .withMaxRecordsPerSecond(5000)
    .withMaxBytesPerSecond(4000000)

  val defaultFlowSettings = KinesisFirehoseFlowSettings.Defaults
  //#flow-settings

  //#flow-sink
  val flow1: Flow[Record, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow("myStreamName")

  val flow2: Flow[Record, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow("myStreamName", flowSettings)

  val sink1: Sink[Record, NotUsed] = KinesisFirehoseSink("myStreamName")
  val sink2: Sink[Record, NotUsed] = KinesisFirehoseSink("myStreamName", flowSettings)
  //#flow-sink

  //#error-handling
  val flowWithErrors: Flow[Record, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow("streamName")
    .map { response =>
      if (response.errorCode() != null) {
        throw new RuntimeException(response.errorCode())
      }
      response
    }
  //#error-handling

}
