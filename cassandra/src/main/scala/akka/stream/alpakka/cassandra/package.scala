/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra

import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }

import scala.concurrent.{ Future, Promise }

package object GuavaFutureOpts {
  private[cassandra] final class GuavaFutureOpts[A](val guavaFut: ListenableFuture[A]) extends AnyVal {
    def asScala(): Future[A] = {
      val p = Promise[A]()
      val callback = new FutureCallback[A] {
        override def onSuccess(a: A): Unit = p.success(a)
        override def onFailure(err: Throwable): Unit = p.failure(err)
      }
      Futures.addCallback(guavaFut, callback)
      p.future
    }
  }

  private[cassandra] implicit def toGuavaFutureOpts[A](guavaFut: ListenableFuture[A]): GuavaFutureOpts[A] =
    new GuavaFutureOpts(guavaFut)
}
