/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.scaladsl

import akka.NotUsed
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings
import akka.stream.scaladsl.Sink
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient
import software.amazon.awssdk.services.firehose.model.Record

object KinesisFirehoseSink {
  def apply(streamName: String, settings: KinesisFirehoseFlowSettings = KinesisFirehoseFlowSettings.Defaults)(implicit
      kinesisClient: FirehoseAsyncClient
  ): Sink[Record, NotUsed] =
    KinesisFirehoseFlow(streamName, settings).to(Sink.ignore)
}
