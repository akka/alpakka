/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import java.util.concurrent.ConcurrentHashMap
import akka.NotUsed
import akka.stream.alpakka.couchbase.{CouchbaseWriteSettings, SingleOperationResult}
import akka.stream.scaladsl.Flow
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import rx.lang.scala.JavaConversions.toScalaObservable
import akka.stream.alpakka.couchbase._
import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.{Failure, Success}
import akka.stream.alpakka.couchbase.impl.CouchbaseSourceImpl
import scala.collection.immutable.Seq

object CouchbaseFlow {

  def fromSingleId[T <: Document[_]](couchbaseBucket: Bucket, target: Class[T]): Flow[String, T, NotUsed] =
    Flow[String].mapAsync(1) { id =>
      val scalaObservable = toScalaObservable(CouchbaseSourceImpl.fromSingleId(id, couchbaseBucket, target))
      val promise = Promise[T]
      scalaObservable.subscribe(
        result => promise.success(result),
        ex => promise.failure(ex)
      )
      promise.future

    }

  def fromBulkIds[T <: Document[_]](couchbaseBucket: Bucket, target: Class[T]): Flow[Seq[String], Seq[T], NotUsed] =
    Flow[Seq[String]].mapAsync(1) { ids =>
      val scalaObservable = CouchbaseSourceImpl.fromBulkIds(ids, couchbaseBucket, target).toSeq
      val promise = Promise[Seq[T]]
      scalaObservable.subscribe(
        docs => promise.success(docs.to[Seq]),
        ex => promise.failure(ex)
      )
      promise.future
    }

  def upsertSingle[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                     couchbaseBucket: Bucket): Flow[T, SingleOperationResult[T], NotUsed] =
    Flow[T]
      .mapAsync(couchbaseWriteSettings.parallelism) { doc =>
        val javaObservable = couchbaseBucket
          .async()
          .upsert(doc,
                  couchbaseWriteSettings.persistTo,
                  couchbaseWriteSettings.replicateTo,
                  couchbaseWriteSettings.timeout,
                  couchbaseWriteSettings.timeUnit)
          .single()
        val promise = Promise[SingleOperationResult[T]]
        val scalaObservable = toScalaObservable(javaObservable)
        scalaObservable.subscribe(
          _ => promise.success(SingleOperationResult(doc, Success(doc.id))),
          e => promise.success(SingleOperationResult(doc, Failure(e)))
        )
        promise.future
      }

  def upsertBulk[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                   couchbaseBucket: Bucket): Flow[Seq[T], BulkOperationResult[T], NotUsed] =
    Flow[Seq[T]].mapAsync(couchbaseWriteSettings.parallelism) { docs =>
      val promise = Promise[BulkOperationResult[T]]
      val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
      rx.lang.scala.Observable
        .from(docs)
        .flatMap(
          doc =>
            toScalaObservable(
              couchbaseBucket
                .async()
                .upsert(doc,
                        couchbaseWriteSettings.persistTo,
                        couchbaseWriteSettings.replicateTo,
                        couchbaseWriteSettings.timeout,
                        couchbaseWriteSettings.timeUnit)
            ).onErrorResumeNext(ex => {
              failures.put(doc.id(), ex)
              rx.lang.scala.Observable.empty
            })
        )
        .toSeq
        .subscribe(
          _ =>
            promise.success(BulkOperationResult(docs, failures.asScala.map(f => FailedOperation(f._1, f._2)).to[Seq]))
        )
      promise.future
    }

  def deleteOne(couchbaseWriteSettings: CouchbaseWriteSettings,
                couchbaseBucket: Bucket): Flow[String, SingleOperationResult[String], NotUsed] =
    Flow[String].mapAsync(couchbaseWriteSettings.parallelism) { id =>
      val javaObservable = couchbaseBucket
        .async()
        .remove(id.toString,
                couchbaseWriteSettings.persistTo,
                couchbaseWriteSettings.replicateTo,
                couchbaseWriteSettings.timeout,
                couchbaseWriteSettings.timeUnit)
        .single()
      val promise = Promise[SingleOperationResult[String]]
      val scalaObservable = toScalaObservable(javaObservable)
      scalaObservable.subscribe(
        result => promise.success(SingleOperationResult(result.id(), Success(result.id))),
        ex => promise.success(SingleOperationResult(id, Failure(ex)))
      )
      promise.future
    }

  def deleteBulk(couchbaseWriteSettings: CouchbaseWriteSettings,
                 couchbaseBucket: Bucket): Flow[Seq[String], BulkOperationResult[String], NotUsed] = {
    val failures: ConcurrentHashMap[String, Throwable] = new ConcurrentHashMap
    Flow[Seq[String]].mapAsync(couchbaseWriteSettings.parallelism) { ids =>
      val promise = Promise[BulkOperationResult[String]]
      rx.lang.scala.Observable
        .from(ids)
        .flatMap(
          id =>
            toScalaObservable(
              couchbaseBucket
                .async()
                .remove(id.toString,
                        couchbaseWriteSettings.persistTo,
                        couchbaseWriteSettings.replicateTo,
                        couchbaseWriteSettings.timeout,
                        couchbaseWriteSettings.timeUnit)
            ).onErrorResumeNext(ex => {
              failures.put(id, ex)
              rx.lang.scala.Observable.empty
            })
        )
        .toSeq
        .subscribe(
          _ => promise.success(BulkOperationResult(ids, failures.asScala.to[Seq].map(f => FailedOperation(f._1, f._2))))
        )
      promise.future
    }
  }

}
