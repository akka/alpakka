/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
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

  def apply(streamName: String, settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance)(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[PutRecordsRequestEntry, NotUsed] =
    KinesisFlow(streamName, settings).to(Sink.ignore)

  def byPartitionAndData(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[(String, ByteBuffer), NotUsed] =
    KinesisFlow.byPartitionAndData(streamName, settings).to(Sink.ignore)

  def byParititonAndBytes(
      streamName: String,
      settings: KinesisFlowSettings = KinesisFlowSettings.defaultInstance
  )(
      implicit kinesisClient: AmazonKinesisAsync
  ): Sink[(String, ByteString), NotUsed] =
    KinesisFlow.byParititonAndBytes(streamName, settings).to(Sink.ignore)

}
