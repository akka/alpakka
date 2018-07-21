/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.scaladsl

import akka.NotUsed
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings
import akka.stream.scaladsl.Sink
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
import com.amazonaws.services.kinesisfirehose.model.Record

object KinesisFirehoseSink {
  def apply(streamName: String, settings: KinesisFirehoseFlowSettings = KinesisFirehoseFlowSettings.defaultInstance)(
      implicit kinesisClient: AmazonKinesisFirehoseAsync
  ): Sink[Record, NotUsed] =
    KinesisFirehoseFlow(streamName, settings).to(Sink.ignore)
}
