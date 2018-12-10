/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import java.util.function.Predicate

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.streams.alpakka.redis.{RedisHSet, RedisKeyValue, RedisPubSub}
import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.ChannelMessage
import reactor.core.publisher.Flux

import scala.collection.immutable.Seq

object RedisSource {

  def subscribe[K, V](topics: Seq[K],
                      connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] = {
    connection.reactive().subscribe(topics: _*).subscribe()
    Source
      .fromPublisher(
        connection
          .reactive()
          .observeChannels()
          .filter(new Predicate[ChannelMessage[K, V]] {
            override def test(t: ChannelMessage[K, V]): Boolean = topics.contains(t.getChannel)
          })
      )
      .map(f => RedisPubSub(f.getChannel, f.getMessage))
  }

  def psubscribe[K, V](patterns: Seq[K],
                       connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] = {

    connection.reactive().psubscribe(patterns: _*).subscribe()
    Source.fromPublisher(connection.reactive().observeChannels()).map(f => RedisPubSub(f.getChannel, f.getMessage))
  }

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

}
