/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

private[pubsub] object GuavaConversions {
  implicit class ListenableFutureConversion[T](private val f: ListenableFuture[T]) {
    def asScalaFuture: Future[T] = {
      val p = Promise[T]
      Futures.addCallback(f, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = {
          p.failure(t)
          ()
        }
        def onSuccess(result: T): Unit = {
          p.success(result)
          ()
        }
      })
      p.future
    }
  }
}
