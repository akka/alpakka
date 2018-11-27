/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import akka.streams.alpakka.redis.RedisPubSub
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection

import scala.collection.JavaConverters._

object RedisSource {

  def subscribe[K, V](topics: java.util.List[K],
                      connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.subscribe(topics.asScala, connection))

  def psubscribe[K, V](topics: java.util.List[K],
                       connection: StatefulRedisPubSubConnection[K, V]): Source[RedisPubSub[K, V], NotUsed] =
    Source.fromGraph(akka.streams.alpakka.redis.scaladsl.RedisSource.psubscribe(topics.asScala, connection))
}
