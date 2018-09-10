/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow, N1qlQuery, Statement}
import rx.lang.scala.JavaConversions.{toJavaObservable, toScalaObservable}
import rx.{Observable, RxReactiveStreams}

import scala.reflect.{classTag, ClassTag}
import rx.lang.scala

object CouchbaseSource {

  def apply[T <: Document[_]: ClassTag](id: String, couchbaseBucket: Bucket): Source[T, NotUsed] = {
    val observable: Observable[T] =
      couchbaseBucket.async().get(id, classTag[T].runtimeClass.asInstanceOf[Class[T]]).single()
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }

  def apply[T <: Document[_]: ClassTag](ids: Seq[String], couchbaseBucket: Bucket): Source[T, NotUsed] = {
    val scalaObservable: scala.Observable[T] = rx.lang.scala.Observable
      .from(ids)
      .flatMap(
        id => toScalaObservable(couchbaseBucket.async().get(id, classTag[T].runtimeClass.asInstanceOf[Class[T]]))
      )
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(scalaObservable)))
  }

  def apply(statement: Statement, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: scala.Observable[AsyncN1qlQueryResult] = toScalaObservable(couchbaseBucket.async().query(statement))
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable.flatMap(f => f.rows()))))
  }

  def apply(query: N1qlQuery, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: scala.Observable[AsyncN1qlQueryResult] = toScalaObservable(couchbaseBucket.async().query(query))
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable.flatMap(f => f.rows()))))
  }

}
