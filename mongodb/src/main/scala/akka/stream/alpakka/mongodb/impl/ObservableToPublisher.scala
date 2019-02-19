/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.impl

import java.util.concurrent.atomic.AtomicBoolean

import com.mongodb.async.{client => mongoclient}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongodb] final case class ObservableToPublisher[T](observable: mongoclient.Observable[T]) extends Publisher[T] {
  def subscribe(subscriber: Subscriber[_ >: T]): Unit =
    observable.subscribe(new mongoclient.Observer[T]() {
      override def onSubscribe(subscription: mongoclient.Subscription): Unit =
        subscriber.onSubscribe(new Subscription() {
          private final val cancelled: AtomicBoolean = new AtomicBoolean

          override def request(n: Long): Unit =
            if (!subscription.isUnsubscribed && !cancelled.get() && n < 1) {
              subscriber.onError(
                new IllegalArgumentException(
                  s"Demand from publisher should be a positive amount. Current amount is:$n"
                )
              )
            } else {
              subscription.request(n)
            }

          override def cancel(): Unit =
            if (!cancelled.getAndSet(true)) subscription.unsubscribe()
        })

      def onNext(result: T): Unit = subscriber.onNext(result)

      def onError(e: Throwable): Unit = subscriber.onError(e)

      def onComplete(): Unit = subscriber.onComplete()
    })
}
