/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.CouchbaseResponseException
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow}
import rx.functions.Func1
import rx.{Observable, Subscriber}

import scala.concurrent.{Future, Promise}

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object RxUtilities {

  val unfoldDocument = new Func1[AsyncN1qlQueryRow, JsonObject] {
    def call(row: AsyncN1qlQueryRow): JsonObject =
      row.value()
  }

  val failStreamOnError = new Func1[JsonObject, Observable[JsonObject]] {
    override def call(err: JsonObject): Observable[JsonObject] =
      Observable.error(CouchbaseResponseException(err))
  }

  val unfoldJsonObjects = new Func1[AsyncN1qlQueryResult, Observable[JsonObject]] {
    def call(t: AsyncN1qlQueryResult): Observable[JsonObject] = {
      val data: Observable[JsonObject] = t.rows().map(unfoldDocument)
      val errors = t.errors().flatMap(failStreamOnError)
      data.mergeWith(errors)
    }
  }

  def singleObservableToFuture[T](o: Observable[T], id: Any): Future[T] = {
    val p = Promise[T]
    o.single()
      .subscribe(new Subscriber[T]() {
        override def onCompleted(): Unit = p.tryFailure(new RuntimeException(s"No document found for $id"))
        override def onError(e: Throwable): Unit = p.tryFailure(e)
        override def onNext(t: T): Unit = p.trySuccess(t)
      })
    p.future
  }

  def zeroOrOneObservableToFuture[T](o: Observable[T]): Future[Option[T]] = {
    val p = Promise[Option[T]]
    o.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.trySuccess(None)
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.trySuccess(Some(t))
    })
    p.future
  }

  def func1Observable[T, R](fun: T => Observable[R]) =
    new Func1[T, Observable[R]]() {
      override def call(b: T): Observable[R] = fun(b)
    }

  def func1[T, R](fun: T => R) =
    new Func1[T, R]() {
      override def call(b: T): R = fun(b)
    }

}
