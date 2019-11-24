/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.KinesisErrors.NoShardsError
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.impl.KinesisSourceStage
import akka.stream.scaladsl.{Merge, Source}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record

object KinesisSource {

  /**
   * Read from one shard into a stream.
   */
  def basic(shardSettings: ShardSettings, KinesisAsyncClient: KinesisAsyncClient): Source[Record, NotUsed] =
    Source.fromGraph(new KinesisSourceStage(shardSettings, KinesisAsyncClient))

  /**
   * Read from multiple shards into a single stream.
   */
  def basicMerge(shardSettings: List[ShardSettings],
                 KinesisAsyncClient: KinesisAsyncClient): Source[Record, NotUsed] = {
    require(shardSettings.nonEmpty, "shard settings need to be specified")
    val create: ShardSettings => Source[Record, NotUsed] = basic(_, KinesisAsyncClient)
    shardSettings match {
      case Nil => Source.failed(NoShardsError)
      case first :: Nil => create(first)
      case first :: second :: Nil => Source.combine(create(first), create(second))(Merge(_))
      case first :: second :: rest =>
        Source.combine(create(first), create(second), rest.map(create(_)): _*)(Merge(_))
    }
  }

}
