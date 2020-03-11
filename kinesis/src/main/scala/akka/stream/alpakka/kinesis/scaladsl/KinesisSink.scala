/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: KinesisAsyncClient
  ): Sink[PutRecordsRequestEntry, NotUsed] =
    KinesisFlow(streamName, settings).to(Sink.ignore)

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: KinesisAsyncClient
  ): Sink[(String, ByteBuffer), NotUsed] =
    KinesisFlow.byPartitionAndData(streamName, settings).to(Sink.ignore)

  def byPartitionAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: KinesisAsyncClient
  ): Sink[(String, ByteString), NotUsed] =
    KinesisFlow.byPartitionAndBytes(streamName, settings).to(Sink.ignore)

}
