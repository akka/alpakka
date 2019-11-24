/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.javadsl

import akka.NotUsed
import akka.stream.alpakka.kinesis.{scaladsl, ShardSettings}
import akka.stream.javadsl.Source
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record

import scala.collection.JavaConverters._

object KinesisSource {

  /**
   * Read from one shard into a stream.
   */
  def basic(shardSettings: ShardSettings, KinesisAsyncClient: KinesisAsyncClient): Source[Record, NotUsed] =
    scaladsl.KinesisSource.basic(shardSettings, KinesisAsyncClient).asJava

  /**
   * Read from multiple shards into a single stream.
   */
  def basicMerge(shardSettings: java.util.List[ShardSettings],
                 KinesisAsyncClient: KinesisAsyncClient): Source[Record, NotUsed] =
    scaladsl.KinesisSource.basicMerge(shardSettings.asScala.toList, KinesisAsyncClient).asJava

}
