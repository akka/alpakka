/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import java.util.concurrent.ConcurrentHashMap

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.{Bucket, PersistTo, ReplicateTo}
import rx.lang.scala.JavaConversions.toScalaObservable
import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

object CouchbaseFlow {

  def upsertSingle[T <: Document[_]](parallelism: Int, couchbaseBucket: Bucket): Flow[T, (T, Try[String]), NotUsed] =
    Flow[T]
      .mapAsync(parallelism)(doc => {
        val javaObservable = couchbaseBucket.async().upsert(doc).single()
        val promise = Promise[(T, Try[String])]
        val scalaObservable = toScalaObservable(javaObservable)
        scalaObservable.subscribe(
          _ => promise.success((doc, Success(doc.id))),
          e => promise.success((doc, Failure(e))),
        )
        promise.future
      })

  def upsertSingle[T <: Document[_]](parallelism: Int,
                                     couchbaseBucket: Bucket,
                                     persistTo: PersistTo,
                                     replicateTo: ReplicateTo): Flow[T, (T, Try[String]), NotUsed] =
    Flow[T]
      .mapAsync(parallelism)(doc => {
        val javaObservable = couchbaseBucket.async().upsert(doc, persistTo, replicateTo).single()
        val promise = Promise[(T, Try[String])]
        val scalaObservable = toScalaObservable(javaObservable)
        scalaObservable.subscribe(
          _ => promise.success((doc, Success(doc.id))),
          e => promise.success((doc, Failure(e))),
        )
        promise.future
      })

  def upsertBulk[T <: Document[_]](parallelism: Int,
                                   couchbaseBucket: Bucket): Flow[Seq[T], (Seq[T], Seq[(String, Throwable)]), NotUsed] =
    Flow[Seq[T]].mapAsync(parallelism)(docs => {
      val promise = Promise[(Seq[T], Seq[(String, Throwable)])]
      val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
      rx.lang.scala.Observable
        .from(docs)
        .flatMap(
          doc =>
            couchbaseBucket
              .async()
              .upsert(doc)
              .onErrorResumeNext(ex => {
                failures.put(doc.id(), ex)
                rx.Observable.empty()
              })
        )
        .toSeq
        .subscribe(
          _ => promise.success((docs, failures.asScala.toSeq)),
        )
      promise.future
    })

  def upsertBulk[T <: Document[_]](
      parallelism: Int,
      couchbaseBucket: Bucket,
      persistTo: PersistTo,
      replicateTo: ReplicateTo
  ): Flow[Seq[T], (Seq[T], Seq[(String, Throwable)]), NotUsed] =
    Flow[Seq[T]].mapAsync(parallelism)(docs => {
      val promise = Promise[(Seq[T], Seq[(String, Throwable)])]
      val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
      rx.lang.scala.Observable
        .from(docs)
        .flatMap(
          doc =>
            couchbaseBucket
              .async()
              .upsert(doc, persistTo, replicateTo)
              .onErrorResumeNext(ex => {
                failures.put(doc.id(), ex)
                rx.Observable.empty()
              })
        )
        .toSeq
        .subscribe(
          _ => promise.success((docs, failures.asScala.toSeq)),
        )
      promise.future
    })

  def deleteOne(parallelism: Int, couchbaseBucket: Bucket): Flow[String, (String, Try[String]), NotUsed] =
    Flow[String].mapAsync(parallelism)(id => {
      val javaObservable = couchbaseBucket.async().remove(id.toString).single()
      val promise = Promise[(String, Try[String])]
      val scalaObservable = toScalaObservable(javaObservable)
      scalaObservable.subscribe(
        result => promise.success((result.id(), Success(result.id()))),
        ex => promise.success((id, Failure(ex))),
      )
      promise.future
    })

  def deleteOne(parallelism: Int,
                couchbaseBucket: Bucket,
                persistTo: PersistTo,
                replicateTo: ReplicateTo): Flow[String, (String, Try[String]), NotUsed] =
    Flow[String].mapAsync(parallelism)(id => {
      val javaObservable = couchbaseBucket.async().remove(id.toString, persistTo, replicateTo).single()
      val promise = Promise[(String, Try[String])]
      val scalaObservable = toScalaObservable(javaObservable)
      scalaObservable.subscribe(
        result => promise.success((result.id(), Success(result.id))),
        ex => promise.success((id, Failure(ex))),
      )
      promise.future
    })

  def deleteBulk(parallelism: Int,
                 couchbaseBucket: Bucket): Flow[Seq[String], (Seq[String], Seq[(String, Throwable)]), NotUsed] = {
    val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
    Flow[Seq[String]].mapAsync(parallelism)(ids => {
      val promise = Promise[(Seq[String], Seq[(String, Throwable)])]
      rx.lang.scala.Observable
        .from(ids)
        .flatMap(
          id =>
            couchbaseBucket
              .async()
              .remove(id.toString)
              .onErrorResumeNext(ex => {
                failures.put(id, ex)
                rx.Observable.empty()
              })
        )
        .toSeq
        .subscribe(
          _ => promise.success((ids, failures.asScala.toSeq)),
        )
      promise.future
    })
  }

  def deleteBulk(parallelism: Int,
                 couchbaseBucket: Bucket,
                 persistTo: PersistTo,
                 replicateTo: ReplicateTo): Flow[Seq[String], (Seq[String], Seq[(String, Throwable)]), NotUsed] = {
    val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
    Flow[Seq[String]].mapAsync(parallelism)(ids => {
      val promise = Promise[(Seq[String], Seq[(String, Throwable)])]
      rx.lang.scala.Observable
        .from(ids)
        .flatMap(
          id =>
            couchbaseBucket
              .async()
              .remove(id.toString, persistTo, replicateTo)
              .onErrorResumeNext(ex => {
                failures.put(id, ex)
                rx.Observable.empty()
              })
        )
        .toSeq
        .subscribe(
          _ => promise.success((ids, failures.asScala.toSeq)),
        )
      promise.future
    })
  }

}
