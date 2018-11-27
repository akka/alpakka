/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.streams.alpakka.redis.RedisPubSub
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.ChannelMessage

object RedisSource {

  def subscribe[K, V](topics: Seq[K],
                      connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] = {
    connection.reactive().subscribe(topics: _*).subscribe()
    Source
      .fromPublisher(
        connection.reactive().observeChannels().filter((t: ChannelMessage[K, V]) => topics.contains(t.getChannel))
      )
      .map(f => RedisPubSub(f.getChannel, f.getMessage))
  }

  def psubscribe[K, V](patterns: Seq[K],
                       connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] = {

    connection.reactive().psubscribe(patterns: _*).subscribe()
    Source.fromPublisher(connection.reactive().observeChannels()).map(f => RedisPubSub(f.getChannel, f.getMessage))
  }
}
