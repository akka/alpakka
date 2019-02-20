/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.KinesisErrors.NoShardsError
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.impl.KinesisSourceStage
import akka.stream.scaladsl.{Merge, Source}
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model.Record

object KinesisSource {

  /**
   * Read from one shard into a stream.
   */
  def basic(shardSettings: ShardSettings, amazonKinesisAsync: AmazonKinesisAsync): Source[Record, NotUsed] =
    Source.fromGraph(new KinesisSourceStage(shardSettings, amazonKinesisAsync))

  /**
   * Read from multiple shards into a single stream.
   */
  def basicMerge(shardSettings: List[ShardSettings],
                 amazonKinesisAsync: AmazonKinesisAsync): Source[Record, NotUsed] = {
    require(shardSettings.nonEmpty, "shard settings need to be specified")
    val create: ShardSettings => Source[Record, NotUsed] = basic(_, amazonKinesisAsync)
    shardSettings match {
      case Nil => Source.failed(NoShardsError)
      case first :: Nil => create(first)
      case first :: second :: Nil => Source.combine(create(first), create(second))(Merge(_))
      case first :: second :: rest =>
        Source.combine(create(first), create(second), rest.map(create(_)): _*)(Merge(_))
    }
  }

}
