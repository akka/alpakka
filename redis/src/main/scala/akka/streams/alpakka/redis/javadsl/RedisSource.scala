/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import akka.streams.alpakka.redis.{RedisHSet, RedisKeyValue, RedisPubSub, RedisSubscriberSettings}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext

object RedisSource {

  def subscribe[K, V](topics: java.util.List[K],
                      connection: StatefulRedisPubSubConnection[K, V],
                      settings: RedisSubscriberSettings): Source[RedisPubSub[K, V], NotUsed] =
    Source.fromGraph(
      akka.streams.alpakka.redis.scaladsl.RedisSource.subscribe(topics.asScala.to[Seq], connection, settings)
    )

  def get[K, V](key: K, connection: StatefulRedisConnection[K, V]): Source[RedisKeyValue[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.get(key, connection))

  def mget[K, V](keys: java.util.List[K],
                 connection: StatefulRedisConnection[K, V],
                 executionContext: ExecutionContext): Source[RedisKeyValue[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.mget(keys.asScala.to[Seq], connection))

  def hget[K, V](key: K, field: K, connection: StatefulRedisPubSubConnection[K, V]): Source[RedisHSet[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.hget(key, field, connection))

  def hmget[K, V](key: K,
                  fields: java.util.List[K],
                  connection: StatefulRedisPubSubConnection[K, V]): Source[RedisKeyValue[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.hmget(key, fields.asScala.to[Seq], connection))

  def hgetall[K, V](key: K,
                    connection: StatefulRedisConnection[K, V]): Source[java.util.List[RedisKeyValue[K, V]], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.hgetall(key, connection)).map(_.asJava)
}
