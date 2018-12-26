/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.impl.CouchbaseSourceImpl
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow, N1qlQuery, Statement}
import rx.lang.scala.JavaConversions.{toJavaObservable, toScalaObservable}
import rx.{Observable, RxReactiveStreams}
import scala.collection.immutable.Seq
import rx.lang.scala

object CouchbaseSource {

  def fromSingleId[T <: Document[_]](id: String, couchbaseBucket: Bucket, target: Class[T]): Source[T, NotUsed] = {
    val observable: Observable[T] =
      CouchbaseSourceImpl.fromSingleId(id, couchbaseBucket, target)
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }

  def fromBulkIds[T <: Document[_]](ids: Seq[String], couchbaseBucket: Bucket, target: Class[T]): Source[T, NotUsed] = {
    val observable: scala.Observable[T] = CouchbaseSourceImpl.fromBulkIds(ids, couchbaseBucket, target)
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable)))
  }

  def fromStatement(statement: Statement, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: rx.lang.scala.Observable[AsyncN1qlQueryResult] = toScalaObservable(
      couchbaseBucket.async().query(statement)
    )
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable.flatMap(f => f.rows()))))
  }

  def fromN1qlQuery(query: N1qlQuery, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: rx.lang.scala.Observable[AsyncN1qlQueryResult] = toScalaObservable(
      couchbaseBucket.async().query(query)
    )
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(observable.flatMap(f => f.rows()))))
  }
}
