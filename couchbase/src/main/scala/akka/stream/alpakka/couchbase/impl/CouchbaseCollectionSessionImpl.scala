package akka.stream.alpakka.couchbase.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseCollectionSession, CouchbaseSession}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.codec.{RawBinaryTranscoder, RawStringTranscoder}
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv._
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}
import rx.{Observable, RxReactiveStreams}

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters.CompletionStageOps

@InternalApi
class CouchbaseCollectionSessionImpl(bucketSession: CouchbaseSession, scopeName: String, collectionName: String) extends CouchbaseCollectionSession{

  override def bucket: CouchbaseSession = bucketSession

  override def scope: AsyncScope = bucket.underlying.scope(scopeName)
  override def underlying: AsyncCollection = scope.collection(collectionName)

  override def asJava = new CouchbaseCollectionSessionJavaAdapter(this)

  override def insert[T] (document: (String, T)): Future[(String, T)] = {
    if (document._2.isInstanceOf[Array[Byte]]) {
      underlying.insert(document._1, document._2, InsertOptions.insertOptions().transcoder(RawBinaryTranscoder.INSTANCE))
        .thenApply(_ => document).asScala
    } else {
      underlying.insert(document._1, document._2)
        .thenApply(r => document).asScala
    }
  }

  override def insert[T] (document: (String, T), insertOptions: InsertOptions): Future[(String, T)] = {
    underlying.insert(document._1, document._2, insertOptions)
      .thenApply(r => document).asScala
  }

  override def get[T](id: String, target: Class[T]): Future[(String, T)] = {
    val opts = GetOptions.getOptions
    if (target.isAssignableFrom(classOf[Array[Byte]])) {
      opts.transcoder(RawBinaryTranscoder.INSTANCE)
    } else if (target.isAssignableFrom(classOf[String])) {
      opts.transcoder(RawStringTranscoder.INSTANCE)
    }

    underlying.get(id, opts).thenApply(gr => {
        (id, gr.contentAs(target))
      }).asScala
  }

  override def getDocument(id: String): Future[(String, JsonValue)] =
    underlying.get(id).thenApply(gr => (id, asJsonValue(gr))).asScala

  private def asJsonValue(gr: GetResult) =
    try {
      gr.contentAsObject().asInstanceOf[JsonValue]
    } catch {
      case ex: Exception => gr.contentAsArray().asInstanceOf[JsonValue]
    }

  override def getBytes(id: String): Future[(String, Array[Byte])] =
    underlying.get(id).thenApply(gr => (id, gr.contentAsBytes())).asScala

  override def getDocument(id: String, timeout: FiniteDuration): Future[(String, JsonValue)] =
    underlying.get(id)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(gr => (id, asJsonValue(gr)))
      .asScala

  override def getBytes(id: String, timeout: FiniteDuration): Future[(String, Array[Byte])] =
    underlying.get(id)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(gr => (id, gr.contentAsBytes()))
      .asScala

  override def upsert[T](document: (String, T)): Future[(String, T)] = {
    if (document._2.isInstanceOf[Array[Byte]]) {
      underlying.upsert(document._1, document._2, UpsertOptions.upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE))
        .thenApply(_ => document)
        .asScala
    } else if (document._2.isInstanceOf[String]) {
      underlying.upsert(document._1, document._2, UpsertOptions.upsertOptions().transcoder(RawStringTranscoder.INSTANCE))
        .thenApply(_ => document)
        .asScala
    } else {
      underlying.upsert(document._1, document._2)
        .thenApply(_ => document)
        .asScala
    }
  }

  override def upsert[T](document: (String, T), upsertOptions: UpsertOptions): Future[(String, T)] =
    underlying.upsert(document._1, document._2, upsertOptions)
      .thenApply(_ => document)
      .asScala

  override def upsert[T](document: (String, T), upsertOptions: UpsertOptions, timeout: FiniteDuration): Future[(String, T)] =
    underlying.upsert(document._1, document._2, upsertOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => document)
      .asScala


  override def replace[T](document: (String, T)): Future[(String, T)] =
    underlying.replace(document._1, document._2)
      .thenApply(_ => document)
      .asScala

  override def replace[T](document: (String, T), replaceOptions: ReplaceOptions): Future[(String, T)] =
    underlying.replace(document._1, document._2, replaceOptions)
      .thenApply(_ => document)
      .asScala

  override def replace[T](document: (String, T), replaceOptions: ReplaceOptions, timeout: FiniteDuration): Future[(String, T)] =
    underlying.replace(document._1, document._2, replaceOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => document)
      .asScala

  override def remove(id: String): Future[String] =
    underlying.remove(id)
      .thenApply(_ => id)
      .asScala

  override def remove(id: String, removeOptions: RemoveOptions): Future[String] =
    underlying.remove(id, removeOptions)
      .thenApply(_ => id)
      .asScala

  override def remove(id: String, removeOptions: RemoveOptions, timeout: FiniteDuration): Future[String] =
    underlying.remove(id, removeOptions)
      .orTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
      .thenApply(_ => id)
      .asScala

  override def createIndex(indexName: String, createQueryIndexOptions: CreateQueryIndexOptions, fields: String*): Future[Void] =
      underlying
        .queryIndexes()
        .createIndex(indexName, util.Arrays.asList(fields: _*), createQueryIndexOptions)
        .asScala

  override def listIndexes(): Source[QueryIndex, NotUsed] =
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        Observable.from(underlying.queryIndexes().getAllIndexes())
          .flatMap(indexes => Observable.from(indexes))
      )
    )
}
