/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kinesis.scaladsl

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.amazonaws.services.kinesis.model.ShardIteratorType
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.concurrent.duration._

object Examples {

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-system

  //#init-client
  val amazonKinesisAsync: AmazonKinesisAsync = AmazonKinesisAsyncClientBuilder.defaultClient()
  //#init-client

  //#settings
  val settings = ShardSettings(streamName = "myStreamName",
                               shardId = "shard-id",
                               shardIteratorType = ShardIteratorType.TRIM_HORIZON,
                               refreshInterval = 1.second,
                               limit = 500)
  //#settings

  //#single
  KinesisSource.basic(settings, amazonKinesisAsync)
  //#single

  //#list
  val mergeSettings = List(
    ShardSettings("myStreamName",
                  "shard-id-1",
                  ShardIteratorType.AT_SEQUENCE_NUMBER,
                  startingSequenceNumber = Some("sequence"),
                  refreshInterval = 1.second,
                  limit = 500),
    ShardSettings("myStreamName",
                  "shard-id-2",
                  ShardIteratorType.AT_TIMESTAMP,
                  atTimestamp = Some(new Date()),
                  refreshInterval = 1.second,
                  limit = 500)
  )
  KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync)
  //#list

}
