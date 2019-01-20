/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis.impl

import java.util.function.Consumer
import java.{lang, util}
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.streams.alpakka.redis.{RedisKeyValue, RedisSubscriberSettings}
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.output.KeyValueStreamingChannel

/**
 * Internal API
 */
@InternalApi
trait RedisStreamingSourceStage[K, V] extends GraphStage[SourceShape[RedisKeyValue[K, V]]] {

  def connection: StatefulRedisConnection[K, V]
  def settings: RedisSubscriberSettings

  def resultHandler(checkConcurrency: () => Boolean,
                    successCallback: AsyncCallback[RedisKeyValue[K, V]],
                    failureCallback: AsyncCallback[Exception]): RedisFuture[lang.Long]

  val out: Outlet[RedisKeyValue[K, V]] = Outlet("RedisSubscribeSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val maxConcurrency = settings.maxConcurrency

    private var currentRequests = 0

    private val maxBufferSize = settings.maxBufferSize

    private val buffer: util.ArrayDeque[RedisKeyValue[K, V]] = new java.util.ArrayDeque[RedisKeyValue[K, V]]()

    private val successCallback: AsyncCallback[RedisKeyValue[K, V]] =
      getAsyncCallback[RedisKeyValue[K, V]](handleSuccess)

    private val failureCallback: AsyncCallback[Exception] = getAsyncCallback[Exception](handleFailure)

    private var redisFutureCompleted = false

    val result: RedisFuture[lang.Long] = resultHandler(() => checkConcurrency(), successCallback, failureCallback)

    result.thenAcceptAsync(new Consumer[lang.Long] {
      override def accept(t: lang.Long): Unit =
        redisFutureCompleted = true
    })

    setHandler(
      out,
      new OutHandler {

        override def onDownstreamFinish(): Unit =
          super.onDownstreamFinish()

        override def onPull(): Unit =
          if (!buffer.isEmpty) {
            push(out, buffer.poll())
          } else if (shouldTerminateStage) {
            completeStage()
          }
      }
    )

    override def postStop(): Unit =
      super.postStop()

    def handleSuccess(result: RedisKeyValue[K, V]): Unit = {

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
      if (shouldTerminateStage) {
        completeStage()
      }

    }

    private def shouldTerminateStage: Boolean =
      redisFutureCompleted &&
      currentRequests == 0 &&
      buffer.isEmpty

    def checkConcurrency(): Boolean = {
      currentRequests = currentRequests + 1
      maxBufferSize > buffer.size &&
      maxConcurrency > currentRequests
    }

    def handleFailure(ex: Exception): Unit =
      failStage(ex)

  }

  override def shape: SourceShape[RedisKeyValue[K, V]] = SourceShape.of(out)
}

/**
 * Internal API
 */
@InternalApi
private[redis] class RedishmgetSourceStage[K, V](key: K,
                                                 fields: Seq[K],
                                                 val connection: StatefulRedisConnection[K, V],
                                                 val settings: RedisSubscriberSettings =
                                                   RedisSubscriberSettings.Defaults)
    extends RedisStreamingSourceStage[K, V] {

  def resultHandler(checkConcurrency: () => Boolean,
                    successCallback: AsyncCallback[RedisKeyValue[K, V]],
                    failureCallback: AsyncCallback[Exception]): RedisFuture[lang.Long] =
    connection
      .async()
      .hmget(
        new KeyValueStreamingChannel[K, V]() {
          override def onKeyValue(key: K, value: V): Unit =
            if (checkConcurrency()) {
              successCallback.invoke(RedisKeyValue(key, value))
            } else {
              failureCallback.invoke(BufferOverflowException("Redis Source buffer overflown"))
            }
        },
        key,
        fields: _*
      )

}

/**
 * Internal API
 */
@InternalApi
private[redis] class RedishgetallSourceStage[K, V](val key: K,
                                                   override val connection: StatefulRedisConnection[K, V],
                                                   override val settings: RedisSubscriberSettings =
                                                     RedisSubscriberSettings.Defaults)
    extends RedisStreamingSourceStage[K, V] {

  def resultHandler(checkConcurrency: () => Boolean,
                    successCallback: AsyncCallback[RedisKeyValue[K, V]],
                    failureCallback: AsyncCallback[Exception]): RedisFuture[lang.Long] =
    connection
      .async()
      .hgetall(
        new KeyValueStreamingChannel[K, V]() {
          override def onKeyValue(key: K, value: V): Unit =
            if (checkConcurrency()) {
              successCallback.invoke(RedisKeyValue(key, value))
            } else {
              failureCallback.invoke(BufferOverflowException("Redis Source buffer overflown"))
            }
        },
        key
      )

}
