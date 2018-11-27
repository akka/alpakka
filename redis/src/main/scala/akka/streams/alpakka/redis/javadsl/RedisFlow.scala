/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.javadsl

import java.util
import akka.NotUsed
import akka.stream.javadsl.Flow
import akka.streams.alpakka.redis.{RedisKeyValue, RedisKeyValues, RedisOperationResult}
import io.lettuce.core.api.StatefulRedisConnection
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext

object RedisFlow {

  def append[K, V](
      parallelism: Int,
      connection: StatefulRedisConnection[K, V],
      executionContext: ExecutionContext
  ): Flow[RedisKeyValue[K, V], RedisOperationResult[RedisKeyValue[K, V], java.lang.Long], NotUsed] =
    Flow
      .fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.append(parallelism, connection)(executionContext))
      .map(f => RedisOperationResult(f.output, f.result.map(t => java.lang.Long.valueOf(t.toString))))

  def set[K, V](
      parallelism: Int,
      connection: StatefulRedisConnection[K, V],
      executionContext: ExecutionContext
  ): Flow[RedisKeyValue[K, V], RedisOperationResult[RedisKeyValue[K, V], String], NotUsed] =
    Flow.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.set(parallelism, connection)(executionContext))

  def mset[K, V](
      parallelism: Int,
      connection: StatefulRedisConnection[K, V],
      executionContext: ExecutionContext
  ): Flow[java.util.List[RedisKeyValue[K, V]],
          RedisOperationResult[java.util.List[RedisKeyValue[K, V]], String],
          NotUsed] =
    Flow.fromGraph(
      akka.stream.scaladsl
        .Flow[util.List[RedisKeyValue[K, V]]]
        .map(_.asScala.to[Seq])
        .via(akka.streams.alpakka.redis.scaladsl.RedisFlow.mset(parallelism, connection)(executionContext))
        .map(f => f.copy(f.output.asJava))
    )

  def lset[K, V](
      parallelism: Int,
      connection: StatefulRedisConnection[K, V],
      executionContext: ExecutionContext
  ): Flow[RedisKeyValues[K, V], RedisOperationResult[RedisKeyValues[K, V], Long], NotUsed] =
    Flow.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisFlow.lpush(parallelism, connection)(executionContext))

}
