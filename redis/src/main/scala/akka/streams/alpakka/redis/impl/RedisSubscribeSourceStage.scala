/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.impl

import java.util
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.streams.alpakka.redis.{RedisPubSub, RedisSubscriberSettings}
import io.lettuce.core.pubsub.{RedisPubSubListener, StatefulRedisPubSubConnection}
import scala.collection.immutable.Seq

/**
 * Internal API
 */
@InternalApi
private[redis] class RedisSubscribeSourceStage[K, V](topics: Seq[K],
                                                     connection: StatefulRedisPubSubConnection[K, V],
                                                     settings: RedisSubscriberSettings =
                                                       RedisSubscriberSettings.Defaults)
    extends GraphStage[SourceShape[RedisPubSub[K, V]]] {

  val out: Outlet[RedisPubSub[K, V]] = Outlet("RedisSubscribeSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val buffer: util.ArrayDeque[RedisPubSub[K, V]] = new java.util.ArrayDeque[RedisPubSub[K, V]]()

    private val successCallback: AsyncCallback[RedisPubSub[K, V]] = getAsyncCallback[RedisPubSub[K, V]](handleSuccess)
    private val failureCallback: AsyncCallback[Exception] = getAsyncCallback[Exception](handleFailure)

    private val maxConcurrency = settings.maxConcurrency

    private var currentRequests = 0

    private val maxBufferSize = settings.maxBufferSize

    setHandler(
      out,
      new OutHandler {

        override def onDownstreamFinish(): Unit = {
          super.onDownstreamFinish()
          connection.async().unsubscribe(topics: _*)
        }
        override def onPull(): Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.poll())
          }
      }
    )

    def handleSuccess(result: RedisPubSub[K, V]): Unit = {

      currentRequests = currentRequests - 1

      (buffer.isEmpty, isAvailable(out)) match {
        case (false, true) =>
          push(out, buffer.poll())
          buffer.offer(result)
        case (true, true) =>
          push(out, result)
        case (_, false) =>
          buffer.offer(result)
      }

    }

    def checkConcurrency(): Boolean = {
      currentRequests = currentRequests + 1
      maxBufferSize > buffer.size &&
      maxConcurrency > currentRequests
    }

    def handleFailure(ex: Exception): Unit = {
      if (settings.unsubscribeOnShutDown) {
        connection.async().unsubscribe(topics: _*)
      }
      failStage(ex)
    }

    override def preStart(): Unit = {
      val listener: SubscribeListener[K, V] =
        new SubscribeListener[K, V](successCallback, () => checkConcurrency(), failureCallback)
      connection.addListener(listener)
      connection.async().subscribe(topics: _*)
    }

    override def postStop(): Unit = if (settings.unsubscribeOnShutDown) connection.async().unsubscribe(topics: _*)

  }

  override def shape: SourceShape[RedisPubSub[K, V]] = SourceShape.of(out)

}

/**
 * Internal API
 */
@InternalApi
private final class SubscribeListener[K, V](successListener: AsyncCallback[RedisPubSub[K, V]],
                                            checkConcurrency: () => Boolean,
                                            failureCallback: AsyncCallback[Exception])
    extends RedisPubSubListener[K, V] {

  override def message(channel: K, message: V): Unit =
    if (checkConcurrency.apply()) {
      successListener.invoke(RedisPubSub(channel, message))
    } else {
      failureCallback.invoke(BufferOverflowException("Redis Source buffer overflown"))
    }

  override def message(pattern: K, channel: K, message: V): Unit = ()

  override def subscribed(channel: K, count: Long): Unit = ()

  override def psubscribed(pattern: K, count: Long): Unit = ()

  override def unsubscribed(channel: K, count: Long): Unit = ()

  override def punsubscribed(pattern: K, count: Long): Unit = ()
}
