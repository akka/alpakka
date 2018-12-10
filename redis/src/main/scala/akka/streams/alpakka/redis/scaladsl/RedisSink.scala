/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import akka.streams.alpakka.redis.{RedisHSet, RedisKeyValue, RedisKeyValues}
import io.lettuce.core.api.StatefulRedisConnection
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

class RedisSink {

  def set[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[RedisKeyValue[K, V], Future[Done]] =
    RedisFlow.set(parallelism, connection).toMat(Sink.ignore)(Keep.right)

  def apppend[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[RedisKeyValue[K, V], Future[Done]] =
    RedisFlow.append(parallelism, connection).toMat(Sink.ignore)(Keep.right)

  def mset[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[Seq[RedisKeyValue[K, V]], Future[Done]] =
    RedisFlow.mset(parallelism, connection).toMat(Sink.ignore)(Keep.right)

  def lpush[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[RedisKeyValues[K, V], Future[Done]] =
    RedisFlow.lpush(parallelism, connection).toMat(Sink.ignore)(Keep.right)

  def hset[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[RedisHSet[K, V], Future[Done]] =
    RedisFlow.hset(parallelism, connection).toMat(Sink.ignore)(Keep.right)

  def hmset[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Sink[RedisHSet[K, V], Future[Done]] =
    RedisFlow.hset(parallelism, connection).toMat(Sink.ignore)(Keep.right)

}
