/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.impl

import akka.annotation.InternalApi
import akka.streams.alpakka.redis.{RedisPSubscribeResult, RedisPubSub}
import io.lettuce.core.pubsub.RedisPubSubAdapter

import scala.concurrent.Promise

/**
 * Internal API
 */
@InternalApi
private[redis] class AlpakkaPubSubListener[K, V](
    promise: Either[Promise[RedisPubSub[K, V]], Promise[RedisPSubscribeResult[K, V]]]
) extends RedisPubSubAdapter[K, V] {

  override def message(channel: K, message: V): Unit =
    promise.fold(fa => fa.success(RedisPubSub(channel, message)), fb => ())

  override def message(pattern: K, channel: K, message: V): Unit =
    promise.fold(fa => (), fb => fb.success(RedisPSubscribeResult(pattern, channel, message)))

  override def subscribed(channel: K, count: Long): Unit = super.subscribed(channel, count)

  override def psubscribed(pattern: K, count: Long): Unit = super.psubscribed(pattern, count)

  override def unsubscribed(channel: K, count: Long): Unit = super.unsubscribed(channel, count)

  override def punsubscribed(pattern: K, count: Long): Unit = super.punsubscribed(pattern, count)
}
