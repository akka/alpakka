/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.streams.alpakka.redis
import akka.streams.alpakka.redis.{RedisKeyValue, RedisKeyValues, RedisOperationResult, RedisPubSub}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object RedisFlow {

  def set[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisKeyValue[K, V], RedisOperationResult[RedisKeyValue[K, V], String], NotUsed] =
    Flow[RedisKeyValue[K, V]].mapAsync(parallelism) { redisSet =>
      val resultFuture = connection.async().set(redisSet.key, redisSet.value).toScala
      resultFuture.map(r => RedisOperationResult(redisSet, Success(r))).recover {
        case ex: Throwable => RedisOperationResult(redisSet, Failure(ex))
      }
    }

  def mset[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[Seq[RedisKeyValue[K, V]], RedisOperationResult[Seq[RedisKeyValue[K, V]], String], NotUsed] =
    Flow[Seq[RedisKeyValue[K, V]]].mapAsync(parallelism) { sets =>
      val map = sets.map(f => (f.key, f.value)).toMap.asJava
      connection
        .async()
        .mset(map)
        .toScala
        .map(t => RedisOperationResult(sets, Success(t)))
        .recover { case ex => RedisOperationResult(sets, Failure(ex)) }
    }

  def lpush[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisKeyValues[K, V], RedisOperationResult[RedisKeyValues[K, V], Long], NotUsed] =
    Flow[RedisKeyValues[K, V]].mapAsync(parallelism) { keyValues =>
      val resultFuture = connection.async().lpush(keyValues.key, keyValues.values: _*).toScala
      resultFuture.map(r => redis.RedisOperationResult(keyValues, Success(r.longValue()))).recover {
        case ex: Throwable => ex.printStackTrace(); RedisOperationResult(keyValues, Failure(ex))
      }
    }

  def publish[K, V](parallelism: Int, connection: StatefulRedisPubSubConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisPubSub[K, V], RedisOperationResult[RedisPubSub[K, V], Long], NotUsed] =
    Flow[RedisPubSub[K, V]].mapAsync(parallelism) { pubSub =>
      connection
        .async()
        .publish(pubSub.channel, pubSub.value)
        .toScala
        .map(t => RedisOperationResult(pubSub, Success(t.longValue())))
        .recover { case ex => RedisOperationResult(pubSub, Failure(ex)) }
    }

  def append[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisKeyValue[K, V], RedisOperationResult[RedisKeyValue[K, V], Long], NotUsed] =
    Flow[RedisKeyValue[K, V]].mapAsync(parallelism) { redisSet =>
      connection
        .async()
        .append(redisSet.key, redisSet.value)
        .toScala
        .map(r => RedisOperationResult(redisSet, Success(r.longValue())))
        .recover { case ex: Throwable => RedisOperationResult(redisSet, Failure(ex)) }
    }

}
