/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.japi.Pair
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

  def withUserContext[T](
      streamName: String,
      kinesisClient: AmazonKinesisAsync
  ): Flow[Pair[PutRecordsRequestEntry, T], Pair[PutRecordsResultEntry, T], NotUsed] =
    withUserContext(streamName, KinesisFlowSettings.defaultInstance, kinesisClient)

  def withUserContext[T](
      streamName: String,
      settings: KinesisFlowSettings,
      kinesisClient: AmazonKinesisAsync
  ): Flow[Pair[PutRecordsRequestEntry, T], Pair[PutRecordsResultEntry, T], NotUsed] =
    akka.stream.scaladsl
      .Flow[Pair[PutRecordsRequestEntry, T]]
      .map(_.toScala)
      .via(scaladsl.KinesisFlow.withUserContext[T](streamName, settings)(kinesisClient))
      .map({ case (res, ctx) => Pair.create(res, ctx) })
      .asJava
}
