/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.Defaults)(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[PutRecordsRequestEntry, NotUsed] =
    KinesisFlow(streamName, settings).to(Sink.ignore)

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[(String, ByteBuffer), NotUsed] =
    KinesisFlow.byPartitionAndData(streamName, settings).to(Sink.ignore)

  def byPartitionAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[(String, ByteString), NotUsed] =
    KinesisFlow.byPartitionAndBytes(streamName, settings).to(Sink.ignore)

}
