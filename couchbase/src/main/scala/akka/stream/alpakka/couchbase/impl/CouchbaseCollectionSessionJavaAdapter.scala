/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import akka.{Done, NotUsed}
import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.alpakka.couchbase.{CouchbaseDocument, javadsl, scaladsl}
import akka.stream.javadsl.Source
import com.couchbase.client.java.json.{JsonArray, JsonObject, JsonValue}
import com.couchbase.client.java.kv.{InsertOptions, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}

import java.time.Duration
import java.util.concurrent.{CompletionStage, TimeUnit}
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] final class CouchbaseCollectionSessionJavaAdapter(delegate: scaladsl.CouchbaseCollectionSession)
    extends javadsl.CouchbaseCollectionSession {

  override def asScala: scaladsl.CouchbaseCollectionSession = delegate

  override def bucket: CouchbaseSession = delegate.bucket.asJava

  override def scope: AsyncScope = delegate.scope

  override def underlying: AsyncCollection = delegate.underlying

  /**
   * Insert a JSON document using the default write settings
   *
   * @param document A tuple where first element is id of the document and second is its value
   * @return A Future that completes with the id of the written document when the write is done
   */
  override def insert[T](id: String, document: T): CompletionStage[Done] =
    delegate.insert(id, document).asJava

  override def insert[T](id: String, document: T, insertOptions: InsertOptions): CompletionStage[Done] =
    delegate.insert(id, document, insertOptions).asJava

  override def getJsonObject(id: String): CompletionStage[CouchbaseDocument[JsonObject]] =
    delegate.getJsonObject(id).asJava

  override def getJsonArray(id: String): CompletionStage[CouchbaseDocument[JsonArray]] =
    delegate.getJsonArray(id).asJava

  override def get[T](id: String, target: Class[T]): CompletionStage[CouchbaseDocument[T]] =
    delegate.get(id, target).asJava
  /**
   * @return A document if found or none if there is no document for the id
   */
  override def getDocument(id: String): CompletionStage[CouchbaseDocument[JsonValue]] =
    delegate.getDocument(id).asJava

  /**
   * @param id Identifier of the document to fetch
   * @return Raw data for the document or none
   */
  override def getBytes(id: String): CompletionStage[CouchbaseDocument[Array[Byte]]] =
    delegate.getBytes(id).asJava

  /**
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  override def getDocument(id: String, timeout: Duration): CompletionStage[CouchbaseDocument[JsonValue]] =
    delegate.getDocument(id, FiniteDuration.apply(timeout.toNanos, TimeUnit.NANOSECONDS)).asJava

  /**
   * @return A raw document data if found or none if there is no document for the id
   */
  override def getBytes(id: String, timeout: Duration): CompletionStage[CouchbaseDocument[Array[Byte]]] =
    delegate.getBytes(id, FiniteDuration.apply(timeout.toNanos, TimeUnit.NANOSECONDS)).asJava

  /**
   * Upsert using the default write settings.
   *
   * @return a future that completes when the upsert is done
   */
  override def upsert[T](id: String, document: T): CompletionStage[Done] =
    delegate.upsert(id, document).asJava

  /**
   * Upsert using the given write settings
   *
   * Separate from `upsert` to make the most common case smoother with the type inference
   *
   * @return a future that completes when the upsert is done
   */
  override def upsert[T](id: String, document: T, upsertOptions: UpsertOptions): CompletionStage[Done] =
    delegate.upsert(id, document, upsertOptions).asJava

  /**
   * Upsert using given write settings and timeout
   *
   * @param id document id
   * @param document      document value to upsert
   * @param upsertOptions Couchbase UpsertOptions
   * @param timeout       timeout for the operation
   * @return a future that completes after the operation is done
   */
  override def upsert[T](id: String, document: T, upsertOptions: UpsertOptions, timeout: Duration): CompletionStage[Done] =
    delegate.upsert(id, document, upsertOptions, FiniteDuration.apply(timeout.toNanos, TimeUnit.NANOSECONDS)).asJava

  /**
   * Replace using the default write settings.
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  override def replace[T](id: String, document: T): CompletionStage[Done] =
    delegate.replace(id, document).asJava

  /**
   * Replace using the given replace options
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  override def replace[T](id: String, document: T, replaceOptions: ReplaceOptions): CompletionStage[Done] =
    delegate.replace(id, document, replaceOptions).asJava

  /**
   * Replace using write settings and timeout
   *
   * @param id id of the document to replace
   * @param document       document value to replace
   * @param replaceOptions Couchbase replace options
   * @param timeout        timeout for the operation
   * @return a future that completes after the operation is done
   */
  override def replace[T](id: String, document: T, replaceOptions: ReplaceOptions, timeout: Duration): CompletionStage[Done] =
    delegate.replace(id, document, replaceOptions, FiniteDuration.apply(timeout.toNanos, TimeUnit.NANOSECONDS)).asJava

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  override def remove(id: String): CompletionStage[Done] =
    delegate.remove(id).asJava

  /**
   * Remove a document by id using the remove settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  override def remove(id: String, removeOptions: RemoveOptions): CompletionStage[Done] =
    delegate.remove(id, removeOptions).asJava

  /**
   * Removes document with given id, remove options and timeout
   *
   * @param id            id of the document to remove
   * @param removeOptions Couchbase remove options
   * @param timeout       timeout
   * @return the id
   */
  override def remove(id: String, removeOptions: RemoveOptions, timeout: Duration) =
    delegate.remove(id, removeOptions, FiniteDuration.apply(timeout.toNanos, TimeUnit.NANOSECONDS)).asJava

  /**
   * Create a secondary index for the current collection.
   *
   * @param indexName               the name of the index.
   * @param createQueryIndexOptions Couchbase index options
   * @param fields                  the JSON fields to index
   * @return a [[scala.concurrent.Future]] of `true` if the index was/will be effectively created, `false`
   *         if the index existed and `ignoreIfExist` is `true`. Completion of the future does not guarantee the index is online
   *         and ready to be used.
   */
  override def createIndex(indexName: String, createQueryIndexOptions: CreateQueryIndexOptions, fields: String*): CompletionStage[Done] =
    delegate.createIndex(indexName, createQueryIndexOptions, fields: _*).asJava

  /**
   * List the existing secondary indexes for the collection
   */
  override def listIndexes(): Source[QueryIndex, NotUsed] =
    delegate.listIndexes().asJava
}
