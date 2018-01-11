/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, KinesisFlowSettings}
import akka.stream.javadsl.Sink
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {

  def apply(streamName: String, kinesisClient: AmazonKinesisAsync): Sink[PutRecordsRequestEntry, NotUsed] =
    apply(streamName, KinesisFlowSettings.defaultInstance, kinesisClient)

  def apply(streamName: String,
            settings: KinesisFlowSettings,
            kinesisClient: AmazonKinesisAsync): Sink[PutRecordsRequestEntry, NotUsed] =
    (scaladsl.KinesisSink.apply(streamName, settings)(kinesisClient)).asJava

}
