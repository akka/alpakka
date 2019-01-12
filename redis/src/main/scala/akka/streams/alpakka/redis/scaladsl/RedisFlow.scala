/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import java.lang
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.streams.alpakka.redis
import akka.streams.alpakka.redis._
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
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

  def get[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[K, RedisOperationResult[K, V], NotUsed] =
    Flow[K].mapAsync(parallelism) { key =>
      val future = connection.async().get(key).toScala
      future
        .map(v => RedisOperationResult[K, V](key, Success(v)))
        .recover { case ex => RedisOperationResult(key, Failure(ex)) }
    }

  def mget[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[Seq[K], RedisOperationResult[Seq[K], Seq[RedisKeyValue[K, V]]], NotUsed] =
    Flow[Seq[K]].mapAsync(parallelism) { keys =>
      val future = connection.async().mget(keys: _*).toScala
      future
        .map(f => f.asScala.map(t => RedisKeyValue(t.getKey, t.getValue)))
        .map(f => RedisOperationResult(keys, Success(Seq.empty ++ f)))
        .recover { case ex: Throwable => RedisOperationResult(keys, Failure(ex)) }
    }

  def hset[K, V](parallelism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisHSet[K, V], RedisOperationResult[RedisHSet[K, V], lang.Boolean], NotUsed] =
    Flow[RedisHSet[K, V]].mapAsync(parallelism) { redisHSet =>
      val future: Future[lang.Boolean] =
        connection.async().hset(redisHSet.key, redisHSet.field, redisHSet.value).toScala
      future
        .map(f => RedisOperationResult(redisHSet, Success(f)))
        .recover { case ex: Throwable => RedisOperationResult(redisHSet, Failure(ex)) }

    }

  def hmset[K, V](parallism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisHMSet[K, V], RedisOperationResult[RedisHMSet[K, V], String], NotUsed] =
    Flow[RedisHMSet[K, V]].mapAsync(parallism) { redisHMSet =>
      val future =
        connection.async().hmset(redisHMSet.key, redisHMSet.values.map(f => f.key -> f.value).toMap.asJava).toScala
      future
        .map(f => RedisOperationResult(redisHMSet, Success(f)))
        .recover { case ex: Throwable => RedisOperationResult(redisHMSet, Failure(ex)) }

    }

  def hdel[K, V](parallism: Int, connection: StatefulRedisConnection[K, V])(
      implicit executionContext: ExecutionContext
  ): Flow[RedisHKeyFields[K], RedisOperationResult[RedisHKeyFields[K], Long], NotUsed] =
    Flow[RedisHKeyFields[K]].mapAsync(parallism) { keyValues =>
      connection
        .async()
        .hdel(keyValues.key, keyValues.fields: _*)
        .toScala
        .map(f => RedisOperationResult[RedisHKeyFields[K], Long](keyValues, Success(f)))
        .recover { case ex: Throwable => RedisOperationResult(keyValues, Failure(ex)) }
    }

}
