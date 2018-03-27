/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder
import com.amazonaws.services.kinesisfirehose.model.{PutRecordBatchResponseEntry, Record}

import scala.concurrent.duration._

object Examples {

  //#init-client
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()

  implicit val amazonKinesisFirehoseAsync: com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync =
    AmazonKinesisFirehoseAsyncClientBuilder.defaultClient()

  system.registerOnTermination(amazonKinesisFirehoseAsync.shutdown())
  //#init-client

  //#flow-settings
  val flowSettings = KinesisFirehoseFlowSettings(
    parallelism = 1,
    maxBatchSize = 500,
    maxRecordsPerSecond = 5000,
    maxBytesPerSecond = 4000000,
    maxRetries = 5,
    backoffStrategy = KinesisFirehoseFlowSettings.Exponential,
    retryInitialTimeout = 100.millis
  )

  val defaultFlowSettings = KinesisFirehoseFlowSettings.defaultInstance
  //#flow-settings

  //#flow-sink
  val flow1: Flow[Record, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow("myStreamName")

  val flow2: Flow[Record, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow("myStreamName", flowSettings)

  val sink1: Sink[Record, NotUsed] = KinesisFirehoseSink("myStreamName")
  val sink2: Sink[Record, NotUsed] = KinesisFirehoseSink("myStreamName", flowSettings)
  //#flow-sink

}
