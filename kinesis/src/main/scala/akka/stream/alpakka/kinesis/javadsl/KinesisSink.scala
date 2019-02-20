/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, KinesisFlowSettings}
import akka.stream.javadsl.Sink
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {

  def create(streamName: String, kinesisClient: AmazonKinesisAsync): Sink[PutRecordsRequestEntry, NotUsed] =
    create(streamName, KinesisFlowSettings.Defaults, kinesisClient)

  def create(streamName: String,
             settings: KinesisFlowSettings,
             kinesisClient: AmazonKinesisAsync): Sink[PutRecordsRequestEntry, NotUsed] =
    scaladsl.KinesisSink(streamName, settings)(kinesisClient).asJava

}
