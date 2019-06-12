/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import java.util.concurrent.TimeUnit

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.alpakka.couchbase.{javadsl, CouchbaseWriteSettings}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.query.util.IndexInfo
import com.couchbase.client.java.query.{N1qlQuery, Statement}
import com.couchbase.client.java.{AsyncBucket, AsyncCluster}
import rx.RxReactiveStreams

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 *
 * @param cluster if provided, it will be shut down when `close()` is called
 */
@InternalApi
final private[couchbase] class CouchbaseSessionImpl(asyncBucket: AsyncBucket, cluster: Option[AsyncCluster])
    extends CouchbaseSession {
  import RxUtilities._

  override def asJava: javadsl.CouchbaseSession = new CouchbaseSessionJavaAdapter(this)

  override def underlying: AsyncBucket = asyncBucket

  def insert(document: JsonDocument): Future[JsonDocument] = insertDoc(document)

  def insertDoc[T <: Document[_]](document: T): Future[T] =
    singleObservableToFuture(asyncBucket.insert(document), document)

  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    insertDoc(document, writeSettings)

  def insertDoc[T <: Document[_]](document: T, writeSettings: CouchbaseWriteSettings): Future[T] =
    singleObservableToFuture(asyncBucket.insert(document,
                                                writeSettings.persistTo,
                                                writeSettings.replicateTo,
                                                writeSettings.timeout.toMillis,
                                                TimeUnit.MILLISECONDS),
                             document)

  def get(id: String): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id))

  def get[T <: Document[_]](id: String, documentClass: Class[T]): Future[Option[T]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id, documentClass))

  def get(id: String, timeout: FiniteDuration): Future[Option[JsonDocument]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id, timeout.toMillis, TimeUnit.MILLISECONDS))

  def get[T <: Document[_]](id: String,
                            timeout: FiniteDuration,
                            documentClass: Class[T]): scala.concurrent.Future[Option[T]] =
    zeroOrOneObservableToFuture(asyncBucket.get(id, documentClass, timeout.toMillis, TimeUnit.MILLISECONDS))

  def upsert(document: JsonDocument): Future[JsonDocument] = upsertDoc(document)

  def upsertDoc[T <: Document[_]](document: T): Future[T] =
    singleObservableToFuture(asyncBucket.upsert(document), document.id)

  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): Future[JsonDocument] =
    upsertDoc(document, writeSettings)

  def upsertDoc[T <: Document[_]](document: T, writeSettings: CouchbaseWriteSettings): Future[T] =
    singleObservableToFuture(asyncBucket.upsert(document,
                                                writeSettings.persistTo,
                                                writeSettings.replicateTo,
                                                writeSettings.timeout.toMillis,
                                                TimeUnit.MILLISECONDS),
                             document.id)

  def remove(id: String): Future[Done] =
    singleObservableToFuture(asyncBucket.remove(id), id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)

  def remove(id: String, writeSettings: CouchbaseWriteSettings): Future[Done] =
    singleObservableToFuture(asyncBucket.remove(id,
                                                writeSettings.persistTo,
                                                writeSettings.replicateTo,
                                                writeSettings.timeout.toMillis,
                                                TimeUnit.MILLISECONDS),
                             id)
      .map(_ => Done)(ExecutionContexts.sameThreadExecutionContext)

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed] =
    // FIXME verify cancellation works
    Source.fromPublisher(RxReactiveStreams.toPublisher(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))

  def streamedQuery(query: Statement): Source[JsonObject, NotUsed] =
    Source.fromPublisher(RxReactiveStreams.toPublisher(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects)))

  def singleResponseQuery(query: Statement): Future[Option[JsonObject]] =
    singleResponseQuery(N1qlQuery.simple(query))
  def singleResponseQuery(query: N1qlQuery): Future[Option[JsonObject]] =
    zeroOrOneObservableToFuture(asyncBucket.query(query).flatMap(RxUtilities.unfoldJsonObjects))

  def counter(id: String, delta: Long, initial: Long): Future[Long] =
    singleObservableToFuture(asyncBucket.counter(id, delta, initial), id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): Future[Long] =
    singleObservableToFuture(asyncBucket.counter(id,
                                                 delta,
                                                 initial,
                                                 writeSettings.persistTo,
                                                 writeSettings.replicateTo,
                                                 writeSettings.timeout.toMillis,
                                                 TimeUnit.MILLISECONDS),
                             id)
      .map(_.content(): Long)(ExecutionContexts.sameThreadExecutionContext)

  def close(): Future[Done] =
    if (!asyncBucket.isClosed) {
      singleObservableToFuture(asyncBucket.close(), "close")
        .flatMap { _ =>
          cluster match {
            case Some(cluster) =>
              singleObservableToFuture(cluster.disconnect(), "close").map(_ => Done)(ExecutionContexts.global())
            case None => Future.successful(Done)
          }
        }(ExecutionContexts.global())
    } else {
      Future.successful(Done)
    }

  override def toString: String = s"CouchbaseSession(${asyncBucket.name()})"

  override def createIndex(indexName: String, ignoreIfExist: Boolean, fields: AnyRef*): Future[Boolean] =
    singleObservableToFuture(
      asyncBucket
        .bucketManager()
        .flatMap(
          func1Observable[AsyncBucketManager, Boolean](
            _.createN1qlIndex(indexName, ignoreIfExist, false, fields: _*)
              .map(func1(Boolean.unbox))
          )
        ),
      s"Create index: $indexName"
    )

  override def listIndexes(): Source[IndexInfo, NotUsed] =
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        asyncBucket
          .bucketManager()
          .flatMap(
            func1Observable((abm: AsyncBucketManager) => abm.listN1qlIndexes())
          )
      )
    )

}
