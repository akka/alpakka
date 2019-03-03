/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesisfirehose.{scaladsl, KinesisFirehoseFlowSettings}
import akka.stream.javadsl.Sink
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
import com.amazonaws.services.kinesisfirehose.model.Record

object KinesisFirehoseSink {

  def apply(streamName: String, kinesisClient: AmazonKinesisFirehoseAsync): Sink[Record, NotUsed] =
    apply(streamName, KinesisFirehoseFlowSettings.Defaults, kinesisClient)

  def apply(streamName: String,
            settings: KinesisFirehoseFlowSettings,
            kinesisClient: AmazonKinesisFirehoseAsync): Sink[Record, NotUsed] =
    (scaladsl.KinesisFirehoseSink.apply(streamName, settings)(kinesisClient)).asJava

}
