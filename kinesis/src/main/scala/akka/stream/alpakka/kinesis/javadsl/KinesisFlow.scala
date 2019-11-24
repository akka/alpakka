/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.stream.alpakka.kinesis.{scaladsl, KinesisFlowSettings}
import akka.stream.javadsl.Flow
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}

object KinesisFlow {

  def create(streamName: String,
             kinesisClient: KinesisAsyncClient): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    create(streamName, KinesisFlowSettings.Defaults, kinesisClient)

  def create(streamName: String,
             settings: KinesisFlowSettings,
             kinesisClient: KinesisAsyncClient): Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] =
    scaladsl.KinesisFlow
      .apply(streamName, settings)(kinesisClient)
      .asJava

  def withUserContext[T](
      streamName: String,
      kinesisClient: KinesisAsyncClient
  ): Flow[Pair[PutRecordsRequestEntry, T], Pair[PutRecordsResultEntry, T], NotUsed] =
    withUserContext(streamName, KinesisFlowSettings.Defaults, kinesisClient)

  def withUserContext[T](
      streamName: String,
      settings: KinesisFlowSettings,
      kinesisClient: KinesisAsyncClient
  ): Flow[Pair[PutRecordsRequestEntry, T], Pair[PutRecordsResultEntry, T], NotUsed] =
    akka.stream.scaladsl
      .Flow[Pair[PutRecordsRequestEntry, T]]
      .map(_.toScala)
      .via(scaladsl.KinesisFlow.withUserContext[T](streamName, settings)(kinesisClient))
      .map { case (res, ctx) => Pair.create(res, ctx) }
      .asJava
}
