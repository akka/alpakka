/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, KinesisFlowSettings}
import akka.stream.javadsl.Flow
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}

object KinesisFlow {

  def apply(streamName: String,
            kinesisClient: AmazonKinesisAsync): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    apply(streamName, KinesisFlowSettings.defaultInstance, kinesisClient)

  def apply(streamName: String,
            settings: KinesisFlowSettings,
            kinesisClient: AmazonKinesisAsync): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    (scaladsl.KinesisFlow.apply(streamName, settings)(kinesisClient)).asJava

}
