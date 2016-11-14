/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra

import com.google.common.util.concurrent.{ FutureCallback, Futures, ListenableFuture }

import scala.concurrent.{ Future, Promise }

class GuavaFutureOpts[A](guavaFut: ListenableFuture[A]) {
  def toFuture(): Future[A] = {
    val p = Promise[A]()
    val callback = new FutureCallback[A] {
      override def onSuccess(a: A): Unit = p.success(a)
      override def onFailure(err: Throwable): Unit = p.failure(err)
    }
    Futures.addCallback(guavaFut, callback)
    p.future
  }
}

object GuavaFutureOptsImplicits {
  implicit def toGuavaFutureOpts[A](guavaFut: ListenableFuture[A]): GuavaFutureOpts[A] =
    new GuavaFutureOpts[A](guavaFut)
}
