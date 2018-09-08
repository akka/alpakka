/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlQuery, Statement}
import rx.lang.scala.JavaConversions.{toJavaObservable, toScalaObservable}
import rx.{Observable, RxReactiveStreams}
import scala.reflect.{classTag, ClassTag}

object CouchbaseSource {

  def apply[T <: Document[_]: ClassTag](id: String, couchbaseBucket: Bucket): Source[T, NotUsed] = {
    val observable: Observable[T] =
      couchbaseBucket.async().get(id, classTag[T].runtimeClass.asInstanceOf[Class[T]]).single()
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }

  def apply[T <: Document[_]: ClassTag](ids: Seq[String], couchbaseBucket: Bucket): Source[T, NotUsed] = {
    val scalaObservable = rx.lang.scala.Observable
      .from(ids)
      .flatMap(f => toScalaObservable(couchbaseBucket.async().get(f, classTag[T].runtimeClass.asInstanceOf[Class[T]])))
    Source.fromPublisher(RxReactiveStreams.toPublisher(toJavaObservable(scalaObservable)))
  }

  def apply(statement: Statement, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: Observable[AsyncN1qlQueryRow] = couchbaseBucket.async().query(statement).flatMap(f => f.rows())
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }

  def apply(query: N1qlQuery, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] = {
    val observable: Observable[AsyncN1qlQueryRow] = couchbaseBucket.async().query(query).flatMap(f => f.rows())
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }

}
