/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl.impl

import java.util.concurrent.LinkedBlockingQueue

import io.lettuce.core.pubsub.RedisPubSubListener

class SubscurbeListener(subscribeQueue: LinkedBlockingQueue[String], unsubscribeQueue: LinkedBlockingQueue[String])
    extends RedisPubSubListener[String, String] {

  override def message(channel: String, message: String): Unit = ()

  override def message(pattern: String, channel: String, message: String): Unit = println(s"pattern $pattern")

  override def subscribed(channel: String, count: Long): Unit = subscribeQueue.put(channel)

  override def psubscribed(pattern: String, count: Long): Unit = ()

  override def unsubscribed(channel: String, count: Long): Unit = unsubscribeQueue.put(channel)

  override def punsubscribed(pattern: String, count: Long): Unit = ()
}
