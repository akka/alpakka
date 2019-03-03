/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose.scaladsl

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings
import akka.stream.alpakka.kinesisfirehose.impl.KinesisFirehoseFlowStage
import akka.stream.scaladsl.Flow
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync
import com.amazonaws.services.kinesisfirehose.model.{PutRecordBatchResponseEntry, Record}

import scala.collection.JavaConverters._
import scala.collection.immutable.Queue
import scala.concurrent.duration._

object KinesisFirehoseFlow {
  def apply(streamName: String, settings: KinesisFirehoseFlowSettings = KinesisFirehoseFlowSettings.Defaults)(
      implicit kinesisClient: AmazonKinesisFirehoseAsync
  ): Flow[Record, PutRecordBatchResponseEntry, NotUsed] =
    Flow[Record]
      .throttle(settings.maxRecordsPerSecond, 1.second, settings.maxRecordsPerSecond, ThrottleMode.Shaping)
      .throttle(settings.maxBytesPerSecond, 1.second, settings.maxBytesPerSecond, getByteSize, ThrottleMode.Shaping)
      .batch(settings.maxBatchSize, Queue(_))(_ :+ _)
      .via(
        new KinesisFirehoseFlowStage(
          streamName,
          settings.maxRetries,
          settings.backoffStrategy,
          settings.retryInitialTimeout
        )
      )
      .mapAsync(settings.parallelism)(identity)
      .mapConcat(_.getRequestResponses.asScala.toIndexedSeq)
      .filter(_.getErrorCode == null)

  private def getByteSize(record: Record): Int = record.getData.position

}
