/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.streams.alpakka.redis.impl.RedisSubscribeSourceStage
import akka.streams.alpakka.redis.{RedisHSet, RedisKeyValue, RedisPubSub, RedisSubscriberSettings}
import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import reactor.core.publisher.Flux

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object RedisSource {

  def subscribe[K, V](
      topics: Seq[K],
      connection: StatefulRedisPubSubConnection[K, V],
      settings: RedisSubscriberSettings = RedisSubscriberSettings.Defaults
  ): Source[RedisPubSub[K, V], NotUsed] =
    Source.fromGraph(new RedisSubscribeSourceStage(topics, connection, settings))

  def get[K, V](key: K, connection: StatefulRedisConnection[K, V]): Source[RedisKeyValue[K, V], NotUsed] =
    Source.fromPublisher(connection.reactive().get(key)).map(f => RedisKeyValue(key, f))

  def mget[K, V](keys: Seq[K], connection: StatefulRedisConnection[K, V]): Source[RedisKeyValue[K, V], NotUsed] = {
    val result: Flux[KeyValue[K, V]] = connection.reactive().mget(keys: _*)
    Source.fromPublisher(result).map(f => RedisKeyValue(f.getKey, f.getValue))
  }

  def hget[K, V](key: K, field: K, connection: StatefulRedisConnection[K, V]): Source[RedisHSet[K, V], NotUsed] =
    Source.fromPublisher(connection.reactive().hget(key, field)).map(f => RedisHSet(key, field, f))

  def hmget[K, V](key: K,
                  fields: Seq[K],
                  connection: StatefulRedisConnection[K, V]): Source[RedisKeyValue[K, V], NotUsed] =
    Source.fromPublisher(connection.reactive().hmget(key, fields: _*)).map(f => RedisKeyValue(f.getKey, f.getValue))

  def hgetall[K, V](key: K,
                    connection: StatefulRedisConnection[K, V]): Source[scala.Seq[RedisKeyValue[K, V]], NotUsed] =
    Source
      .fromPublisher(connection.reactive().hgetall(key))
      .map(f => f.asScala.map(f => RedisKeyValue(f._1, f._2)).toSeq)

}
