/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, KinesisFlowSettings}
import akka.stream.javadsl.Sink
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {

  def create(streamName: String, kinesisClient: KinesisAsyncClient): Sink[PutRecordsRequestEntry, NotUsed] =
    create(streamName, KinesisFlowSettings.Defaults, kinesisClient)

  def create(streamName: String,
             settings: KinesisFlowSettings,
             kinesisClient: KinesisAsyncClient): Sink[PutRecordsRequestEntry, NotUsed] =
    scaladsl.KinesisSink(streamName, settings)(kinesisClient).asJava

}
