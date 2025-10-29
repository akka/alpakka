/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.{Done, NotUsed}
import akka.annotation.DoNotInherit
import akka.stream.alpakka.couchbase.CouchbaseDocument
import akka.stream.javadsl.Source
import com.couchbase.client.java.json.{JsonArray, JsonObject, JsonValue}
import com.couchbase.client.java.kv.{InsertOptions, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}

import java.time.Duration
import java.util.concurrent.{CompletionStage, Executor}
import scala.reflect.ClassTag

/**
 * Java API: Gives access to Couchbase Collection.
 */
object CouchbaseCollectionSession {

  /**
   * Create a session against the given bucket. The couchbase client used to connect will be created and then closed when
   * the session is closed.
   */
  def create(bucketSession: CouchbaseSession,
             scopeName: String,
             collectionName: String,
             executor: Executor): CompletionStage[CouchbaseSession] = null

}

/**
 * Java API: A Couchbase session allowing querying and interacting with a specific couchbase bucket.
 *
 * Not for user extension.
 */
// must be an abstract class, otherwise static forwarders are missing for companion object if building with Scala 2.11
@DoNotInherit
abstract class CouchbaseCollectionSession {
  def bucket: CouchbaseSession
  def scope: AsyncScope
  def underlying: AsyncCollection
  def asScala: akka.stream.alpakka.couchbase.scaladsl.CouchbaseCollectionSession

  /**
   * Insert a JSON document using the default write settings
   * @param document A tuple where first element is id of the document and second is its value
   * @return A Future that completes with the id of the written document when the write is done
   */
  def insert[T: ClassTag](id: String, document: T): CompletionStage[Done]
  def insert[T: ClassTag](id: String, document: T, insertOptions: InsertOptions): CompletionStage[Done]

  def getJsonObject(id: String): CompletionStage[CouchbaseDocument[JsonObject]]
  def getJsonArray(id: String): CompletionStage[CouchbaseDocument[JsonArray]]
  def get[T: ClassTag](id: String, target: Class[T]): CompletionStage[CouchbaseDocument[T]]

  /**
   * @return A document if found or none if there is no document for the id
   */
  def getDocument(id: String): CompletionStage[CouchbaseDocument[JsonValue]]

  /**
   * @param id Identifier of the document to fetch
   * @return Raw data for the document or none
   */
  def getBytes(id: String): CompletionStage[CouchbaseDocument[Array[Byte]]]

  /**
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  def getDocument(id: String, timeout: Duration): CompletionStage[CouchbaseDocument[JsonValue]]

  /**
   * @return A raw document data if found or none if there is no document for the id
   */
  def getBytes(id: String, timeout: Duration): CompletionStage[CouchbaseDocument[Array[Byte]]]

  /**
   * Upsert using the default write settings.
   * @return a future that completes when the upsert is done
   */
  def upsert[T: ClassTag](id: String, document: T): CompletionStage[Done]

  /**
   * Upsert using the given write settings
   *
   * Separate from `upsert` to make the most common case smoother with the type inference
   *
   * @return a future that completes when the upsert is done
   */
  def upsert[T: ClassTag](id: String, document: T, upsertOptions: UpsertOptions): CompletionStage[Done]

  /**
   * Upsert using given write settings and timeout
   * @param id document id
   * @param document document value to upsert
   * @param upsertOptions Couchbase UpsertOptions
   * @param timeout timeout for the operation
   * @return the document id and value
   */
  def upsert[T: ClassTag](id: String,
                          document: T,
                          upsertOptions: UpsertOptions,
                          timeout: Duration): CompletionStage[Done]

  /**
   * Replace using the default write settings.
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T: ClassTag](id: String, document: T): CompletionStage[Done]

  /**
   * Replace using the given replace options
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T: ClassTag](id: String, document: T, replaceOptions: ReplaceOptions): CompletionStage[Done]

  /**
   * Replace using write settings and timeout
   * @param id document id
   * @param document document value to replace
   * @param replaceOptions Couchbase replace options
   * @param timeout timeout for the operation
   * @return the document id and value
   */
  def replace[T: ClassTag](id: String,
                           document: T,
                           replaceOptions: ReplaceOptions,
                           timeout: Duration): CompletionStage[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): CompletionStage[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, removeOptions: RemoveOptions): CompletionStage[Done]

  /**
   * Removes document with given id, remove options and timeout
   * @param id id of the document to remove
   * @param removeOptions Couchbase remove options
   * @param timeout timeout
   * @return the id
   */
  def remove(id: String, removeOptions: RemoveOptions, timeout: Duration): CompletionStage[Done]

  /**
   * Create a secondary index for the current collection.
   *
   * @param indexName the name of the index.
   * @param createQueryIndexOptions Couchbase index options
   * @param fields the JSON fields to index
   * @return a [[CompletionStage]] of `true` if the index was/will be effectively created, `false`
   *      if the index existed and `ignoreIfExist` is `true`. Completion of the future does not guarantee the index is online
   *      and ready to be used.
   */
  def createIndex(indexName: String,
                  createQueryIndexOptions: CreateQueryIndexOptions,
                  fields: String*): CompletionStage[Done]

  /**
   * List the existing secondary indexes for the collection
   */
  def listIndexes(): Source[QueryIndex, NotUsed]
}
