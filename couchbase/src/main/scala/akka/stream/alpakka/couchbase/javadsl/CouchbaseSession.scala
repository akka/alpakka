/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import java.time.Duration
import java.util.Optional
import java.util.concurrent.{CompletionStage, Executor}

import akka.annotation.DoNotInherit
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.impl.CouchbaseSessionJavaAdapter
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession => ScalaDslCouchbaseSession}
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import akka.stream.javadsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.query.util.IndexInfo
import com.couchbase.client.java.query.{N1qlQuery, Statement}
import com.couchbase.client.java.{AsyncBucket, AsyncCluster, Bucket}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

/**
 * Java API: Gives access to Couchbase.
 *
 * @see [[akka.stream.alpakka.couchbase.CouchbaseSessionRegistry]]
 */
object CouchbaseSession {

  /**
   * Create a session against the given bucket. The couchbase client used to connect will be created and then closed when
   * the session is closed.
   */
  def create(settings: CouchbaseSessionSettings,
             bucketName: String,
             executor: Executor): CompletionStage[CouchbaseSession] =
    ScalaDslCouchbaseSession
      .apply(settings, bucketName)(executionContext(executor))
      .map(new CouchbaseSessionJavaAdapter(_): CouchbaseSession)(
        ExecutionContexts.sameThreadExecutionContext
      )
      .toJava

  /**
   * Create a given bucket using a pre-existing cluster client, allowing for it to be shared among
   * multiple `CouchbaseSession`s. The cluster client's life-cycle is the user's responsibility.
   */
  def create(client: AsyncCluster, bucketName: String, executor: Executor): CompletionStage[CouchbaseSession] =
    ScalaDslCouchbaseSession(client, bucketName)(executionContext(executor))
      .map(new CouchbaseSessionJavaAdapter(_): CouchbaseSession)(
        ExecutionContexts.sameThreadExecutionContext
      )
      .toJava

  /**
   * Connects to a Couchbase cluster by creating an `AsyncCluster`.
   * The life-cycle of it is the user's responsibility.
   */
  def createClient(settings: CouchbaseSessionSettings, executor: Executor): CompletionStage[AsyncCluster] =
    ScalaDslCouchbaseSession
      .createClusterClient(settings)(executionContext(executor))
      .toJava

  private def executionContext(executor: Executor): ExecutionContext =
    executor match {
      case ec: ExecutionContext => ec
      case _ => ExecutionContext.fromExecutor(executor)
    }

  /**
   * Create a session against the given bucket. You are responsible for managing the lifecycle of the couchbase client
   * that the bucket was created with.
   */
  def create(bucket: Bucket): CouchbaseSession = new CouchbaseSessionJavaAdapter(ScalaDslCouchbaseSession.apply(bucket))
}

/**
 * Java API: A Couchbase session allowing querying and interacting with a specific couchbase bucket.
 *
 * Not for user extension.
 */
// must be an abstract class, otherwise static forwarders are missing for companion object if building with Scala 2.11
@DoNotInherit
abstract class CouchbaseSession {

  def underlying: AsyncBucket

  def asScala: ScalaDslCouchbaseSession

  /**
   * Insert a JSON document using the default write settings.
   *
   * For inserting other types of documents see `insertDoc`.
   *
   * @return A CompletionStage that completes with the written document when the write completes
   */
  def insert(document: JsonDocument): CompletionStage[JsonDocument]

  /**
   * Insert a document using the default write settings. Separate from `insert` to make the most common
   * case smoother with the type inference
   *
   * @return A CompletionStage that completes with the written document when the write completes
   */
  def insertDoc[T <: Document[_]](document: T): CompletionStage[T]

  /**
   * Insert a JSON document using the given write settings.
   *
   * For inserting other types of documents see `insertDoc`.
   */
  def insert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument]

  /**
   * Insert a document using the given write settings.
   * Separate from `insert` to make the most common case smoother with the type inference
   */
  def insertDoc[T <: Document[_]](document: T, writeSettings: CouchbaseWriteSettings): CompletionStage[T]

  /**
   * @return A document if found or none if there is no document for the id
   */
  def get(id: String): CompletionStage[Optional[JsonDocument]]

  /**
   * @return A document if found or none if there is no document for the id
   */
  def get[T <: Document[_]](id: String, documentClass: Class[T]): CompletionStage[Optional[T]]

  /**
   * @param timeout fail the returned CompletionStage with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  def get(id: String, timeout: Duration): CompletionStage[Optional[JsonDocument]]

  /**
   * @param timeout fail the returned CompletionStage with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  def get[T <: Document[_]](id: String, timeout: Duration, documentClass: Class[T]): CompletionStage[Optional[T]]

  /**
   * Upsert using the default write settings
   *
   * For inserting other types of documents see `upsertDoc`.
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsert(document: JsonDocument): CompletionStage[JsonDocument]

  /**
   * Upsert using the default write settings.
   * Separate from `upsert` to make the most common case smoother with the type inference
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsertDoc[T <: Document[_]](document: T): CompletionStage[T]

  /**
   * Upsert using the given write settings.
   *
   * For inserting other types of documents see `upsertDoc`.
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsert(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument]

  /**
   * Upsert using the given write settings.
   *
   * Separate from `upsert` to make the most common case smoother with the type inference
   *
   * @return a CompletionStage that completes when the upsert is done
   */
  def upsertDoc[T <: Document[_]](document: T, writeSettings: CouchbaseWriteSettings): CompletionStage[T]

  /**
   * Replace using the default write settings
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a CompletionStage that completes when the replace is done
   */
  def replace(document: JsonDocument): CompletionStage[JsonDocument]

  /**
   * Replace using the default write settings.
   * Separate from `replace` to make the most common case smoother with the type inference
   *
   * @return a CompletionStage that completes when the replace is done
   */
  def replaceDoc[T <: Document[_]](document: T): CompletionStage[T]

  /**
   * Replace using the given write settings.
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a CompletionStage that completes when the replace done
   */
  def replace(document: JsonDocument, writeSettings: CouchbaseWriteSettings): CompletionStage[JsonDocument]

  /**
   * Replace using the given write settings.
   *
   * Separate from `replace` to make the most common case smoother with the type inference
   *
   * @return a CompletionStage that completes when the replace is done
   */
  def replaceDoc[T <: Document[_]](document: T, writeSettings: CouchbaseWriteSettings): CompletionStage[T]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return CompletionStage that completes when the document has been removed, if there is no such document
   *         the CompletionStage is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): CompletionStage[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return CompletionStage that completes when the document has been removed, if there is no such document
   *         the CompletionStage is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, writeSettings: CouchbaseWriteSettings): CompletionStage[Done]

  def streamedQuery(query: N1qlQuery): Source[JsonObject, NotUsed]
  def streamedQuery(query: Statement): Source[JsonObject, NotUsed]
  def singleResponseQuery(query: Statement): CompletionStage[Optional[JsonObject]]
  def singleResponseQuery(query: N1qlQuery): CompletionStage[Optional[JsonObject]]

  /**
   * Create or increment a counter
   *
   * @param id What counter document id
   * @param delta Value to increase the counter with if it does exist
   * @param initial Value to start from if the counter does not exist
   * @return The value of the counter after applying the delta
   */
  def counter(id: String, delta: Long, initial: Long): CompletionStage[Long]

  /**
   * Create or increment a counter
   *
   * @param id What counter document id
   * @param delta Value to increase the counter with if it does exist
   * @param initial Value to start from if the counter does not exist
   * @return The value of the counter after applying the delta
   */
  def counter(id: String, delta: Long, initial: Long, writeSettings: CouchbaseWriteSettings): CompletionStage[Long]

  /**
   * Close the session and release all resources it holds. Subsequent calls to other methods will likely fail.
   */
  def close(): CompletionStage[Done]

  /**
   * Create a secondary index for the current bucket.
   *
   * @param indexName the name of the index.
   * @param ignoreIfExist if a secondary index already exists with that name, an exception will be thrown unless this
   *                      is set to true.
   * @param fields the JSON fields to index - each can be either `String` or [[com.couchbase.client.java.query.dsl.Expression]]
   * @return a [[java.util.concurrent.CompletionStage]] of `true` if the index was/will be effectively created, `false`
   *      if the index existed and ignoreIfExist` is true. Completion of the `CompletionStage` does not guarantee the index
   *      is online and ready to be used.
    **/
  def createIndex(indexName: String, ignoreIfExist: Boolean, fields: AnyRef*): CompletionStage[Boolean]

  /**
   * List the existing secondary indexes for the bucket
   */
  def listIndexes(): Source[IndexInfo, NotUsed]
}
