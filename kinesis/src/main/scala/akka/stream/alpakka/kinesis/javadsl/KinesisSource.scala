/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, ShardSettings}
import akka.stream.javadsl.Source
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record

import scala.jdk.CollectionConverters._

object KinesisSource {

  /**
   * Read from one shard into a stream.
   */
  def basic(shardSettings: ShardSettings, amazonKinesisAsync: KinesisAsyncClient): Source[Record, NotUsed] =
    scaladsl.KinesisSource.basic(shardSettings, amazonKinesisAsync).asJava

  /**
   * Read from multiple shards into a single stream.
   */
  def basicMerge(shardSettings: java.util.List[ShardSettings],
                 amazonKinesisAsync: KinesisAsyncClient): Source[Record, NotUsed] =
    scaladsl.KinesisSource.basicMerge(shardSettings.asScala.toList, amazonKinesisAsync).asJava

}
