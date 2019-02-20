/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesisfirehose.{scaladsl, KinesisFirehoseFlowSettings}
import akka.stream.javadsl.Flow
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
import com.amazonaws.services.kinesisfirehose.model.{PutRecordBatchResponseEntry, Record}

object KinesisFirehoseFlow {

  def apply(streamName: String,
            kinesisClient: AmazonKinesisFirehoseAsync): Flow[Record, PutRecordBatchResponseEntry, NotUsed] =
    apply(streamName, KinesisFirehoseFlowSettings.Defaults, kinesisClient)

  def apply(streamName: String,
            settings: KinesisFirehoseFlowSettings,
            kinesisClient: AmazonKinesisFirehoseAsync): Flow[Record, PutRecordBatchResponseEntry, NotUsed] =
    scaladsl.KinesisFirehoseFlow.apply(streamName, settings)(kinesisClient).asJava

}
