/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.streams.alpakka.redis.{RedisKeyValue, RedisKeyValues}
import io.lettuce.core.api.StatefulRedisConnection

import scala.concurrent.ExecutionContext

object RedisSink {

  def set[K, V](parallelism: Int,
                connection: StatefulRedisConnection[K, V],
                executionContext: ExecutionContext): Sink[RedisKeyValue[K, V], CompletionStage[Done]] =
    Flow
      .fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.set(parallelism, connection)(executionContext))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def append[K, V](parallelism: Int,
                   connection: StatefulRedisConnection[K, V],
                   executionContext: ExecutionContext): Sink[RedisKeyValue[K, V], CompletionStage[Done]] =
    Flow
      .fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.append(parallelism, connection)(executionContext))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def mset[K, V](parallelism: Int,
                 connection: StatefulRedisConnection[K, V],
                 executionContext: ExecutionContext): Sink[java.util.List[RedisKeyValue[K, V]], CompletionStage[Done]] =
    Flow
      .fromGraph(RedisFlow.mset(parallelism, connection, executionContext))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def lpush[K, V](parallelism: Int,
                  connection: StatefulRedisConnection[K, V],
                  executionContext: ExecutionContext): Sink[RedisKeyValues[K, V], CompletionStage[Done]] =
    Flow
      .fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.lpush(parallelism, connection)(executionContext))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

}
