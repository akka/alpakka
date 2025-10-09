/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.CouchbaseDocument
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseCollectionSession, CouchbaseSession}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.codec.{RawBinaryTranscoder, RawStringTranscoder, Transcoder}
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv._
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}
import rx.{Observable, RxReactiveStreams}

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] class CouchbaseCollectionSessionImpl(bucketSession: CouchbaseSession,
                                                        scopeName: String,
                                                        collectionName: String)
    extends CouchbaseCollectionSession {

  override def bucket: CouchbaseSession = bucketSession

  override def scope: AsyncScope = bucket.underlying.scope(scopeName)
  override def underlying: AsyncCollection = scope.collection(collectionName)

  override def asJava = new CouchbaseCollectionSessionJavaAdapter(this)

  override def insert[T](id: String, document: T): Future[Done] = {
    underlying
      .insert(id,
              document,
              InsertOptions
                .insertOptions()
                .transcoder(chooseTranscoder(document.getClass)))
      .asScala
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  override def insert[T](id: String, document: T, insertOptions: InsertOptions): Future[Done] = {
    if (insertOptions.build.transcoder() == null) {
      insertOptions.transcoder(chooseTranscoder(document.getClass))
    }
    underlying
      .insert(id, document, insertOptions)
      .asScala
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  override def get[T](id: String, target: Class[T]): Future[CouchbaseDocument[T]] = {
    underlying
      .get(id, GetOptions.getOptions.transcoder(chooseTranscoder(target)))
      .thenApply(gr => new CouchbaseDocument[T](id, gr.contentAs(target)))
      .asScala
  }

  override def getDocument(id: String): Future[CouchbaseDocument[JsonValue]] =
    underlying.get(id).thenApply(gr => new CouchbaseDocument[JsonValue](id, asJsonValue(gr))).asScala

  private def asJsonValue(gr: GetResult) =
    try {
      gr.contentAsObject().asInstanceOf[JsonValue]
    } catch {
      case ex: Exception => gr.contentAsArray().asInstanceOf[JsonValue]
    }

  override def getBytes(id: String): Future[CouchbaseDocument[Array[Byte]]] =
    underlying.get(id).thenApply(gr => new CouchbaseDocument[Array[Byte]](id, gr.contentAsBytes())).asScala

  override def getDocument(id: String, timeout: FiniteDuration): Future[CouchbaseDocument[JsonValue]] =
    underlying
      .get(id)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(gr => new CouchbaseDocument[JsonValue](id, asJsonValue(gr)))
      .asScala

  override def getBytes(id: String, timeout: FiniteDuration): Future[CouchbaseDocument[Array[Byte]]] =
    underlying
      .get(id)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(gr => new CouchbaseDocument[Array[Byte]](id, gr.contentAsBytes()))
      .asScala

  override def upsert[T](id: String, document: T): Future[Done] = {
    underlying
      .upsert(id,
              document,
              UpsertOptions
                .upsertOptions()
                .transcoder(chooseTranscoder(document.getClass)))
      .asScala
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  override def upsert[T](id: String, document: T, upsertOptions: UpsertOptions): Future[Done] = {
    if (upsertOptions.build().transcoder() == null) {
      upsertOptions.transcoder(chooseTranscoder(document.getClass))
    }
    underlying
        .upsert(id, document, upsertOptions)
        .thenApply(_ => Done)
        .asScala
  }

  override def upsert[T](id: String, document: T, upsertOptions: UpsertOptions, timeout: FiniteDuration): Future[Done] = {
    if (upsertOptions.build().transcoder() == null) {
      upsertOptions.transcoder(chooseTranscoder(document.getClass))
    }
    underlying
      .upsert(id, document, upsertOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => Done)
      .asScala
  }

  override def replace[T](id: String, document: T): Future[Done] =
    underlying
      .replace(id, document)
      .thenApply(_ => Done)
      .asScala

  override def replace[T](id: String, document: T, replaceOptions: ReplaceOptions): Future[Done] = {
    if (replaceOptions.build.transcoder() == null) {
      replaceOptions.transcoder(chooseTranscoder(document.getClass))
    }
    underlying
      .replace(id, document, replaceOptions)
      .thenApply(_ => Done)
      .asScala
  }

  override def replace[T](id: String,
                          document: T,
                          replaceOptions: ReplaceOptions,
                          timeout: FiniteDuration): Future[Done] = {
    if (replaceOptions.build.transcoder() == null) {
      replaceOptions.transcoder(chooseTranscoder(document.getClass))
    }
    underlying
      .replace(id, document, replaceOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => Done)
      .asScala
  }

  override def remove(id: String): Future[Done] =
    underlying
      .remove(id)
      .thenApply(_ => Done)
      .asScala

  override def remove(id: String, removeOptions: RemoveOptions): Future[Done] =
    underlying
      .remove(id, removeOptions)
      .thenApply(_ => Done)
      .asScala

  override def remove(id: String, removeOptions: RemoveOptions, timeout: FiniteDuration): Future[Done] =
    underlying
      .remove(id, removeOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => Done)
      .asScala

  override def createIndex(indexName: String,
                           createQueryIndexOptions: CreateQueryIndexOptions,
                           fields: String*): Future[Done] =
    underlying
      .queryIndexes()
      .createIndex(indexName, util.Arrays.asList(fields: _*), createQueryIndexOptions)
      .thenApply(_ => Done)
      .asScala

  override def listIndexes(): Source[QueryIndex, NotUsed] =
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        Observable
          .from(underlying.queryIndexes().getAllIndexes())
          .flatMap(indexes => Observable.from(indexes))
      )
    )

  private def chooseTranscoder[T](target: Class[T]): Transcoder = {
    if (target == classOf[Array[Byte]]) RawBinaryTranscoder.INSTANCE
    else if (target == classOf[String]) RawStringTranscoder.INSTANCE
    else bucketSession.cluster().environment().transcoder()
  }
}
