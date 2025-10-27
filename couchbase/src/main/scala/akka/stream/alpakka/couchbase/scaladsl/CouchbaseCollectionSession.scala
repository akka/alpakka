/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.annotation.DoNotInherit
import akka.stream.alpakka.couchbase.CouchbaseDocument
import akka.stream.alpakka.couchbase.impl.CouchbaseCollectionSessionImpl
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.json.{JsonArray, JsonObject, JsonValue}
import com.couchbase.client.java.kv.{InsertOptions, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object CouchbaseCollectionSession {
  protected def apply(bucket: CouchbaseSession, scope: String, collection: String) =
    new CouchbaseCollectionSessionImpl(bucket, scope, collection)
}

@DoNotInherit
trait CouchbaseCollectionSession {
  def bucket: CouchbaseSession
  def scope: AsyncScope
  def underlying: AsyncCollection
  def asJava: akka.stream.alpakka.couchbase.javadsl.CouchbaseCollectionSession

  /**
   * Insert a document using default InsertOptions and automatically detected Transcoder.
   * Will use RawBinaryTranscoder if document._2 is an array of bytes, RawStringTranscoder if its a String
   * and the default Transcoder otherwise.
   * @param id id of the document
   * @param document value of the document
   * @tparam T type of the document
   * @return a future that completes when the document was written
   */
  def insert[T: ClassTag](id: String, document: T): Future[Done]

  /**
   * Insert a document using provided InsertOptions.
   * @param id id of the  document
   * @param document value of the document
   * @param insertOptions Couchbase InsertOptions
   * @tparam T type of the document
   * @return a future that completes when the document was written or errors if the operation failed
   */
  def insert[T: ClassTag](id: String, document: T, insertOptions: InsertOptions): Future[Done]

  /**
   * Reads a document with given id as JsonObject
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getJsonObject(id: String): Future[CouchbaseDocument[JsonObject]] =
    get[JsonObject](id)

  /**
   * Reads a document with given id as JsonArray
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getJsonArray(id: String): Future[CouchbaseDocument[JsonArray]] = {
    get[JsonArray](id)
  }

  /**
   * Reads a document into an object of target class using appropriate Transcoders:
   *  * If T === Array[Byte] then RawBinaryTranscoder is used
   *  * If T === String then RawStringTranscoder is used
   *  * The default json to class transcoder is used otherwise
   * @param id id of the document
   * @param target target class
   * @tparam T type of the object to return
   * @return a future that completes with created object or errors if the operation failed
   */
  def get[T: ClassTag](id: String): Future[CouchbaseDocument[T]]

  /**
   * Reads a json document as JsonValue (either array or object)
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getDocument(id: String): Future[CouchbaseDocument[JsonValue]]

  /**
   * Reads a document into an array of bytes
   * @param id Identifier of the document to fetch
   * @return a future that completes with Raw data for the document or errors if the operation failed
   */
  def getBytes(id: String): Future[CouchbaseDocument[Array[Byte]]]

  /**
   * Reads a document as JsonValue with given timeout
   * @param id Identifier of the document to fetch
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getDocument(id: String, timeout: FiniteDuration): Future[CouchbaseDocument[JsonValue]]

  /**
   * Reads a document into an array of bytes with given timeout
   * @param id Identifier of the document to fetch
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return a future that completes with Raw data for the document or errors if the operation failed
   */
  def getBytes(id: String, timeout: FiniteDuration): Future[CouchbaseDocument[Array[Byte]]]

  /**
   * Upsert a document using the default write settings.
   * @param id document id
   * @param document value of the document
   * @tparam T type of the document
   * @return a future that completes when the upsert is done
   */
  def upsert[T: ClassTag](id: String, document: T): Future[Done]

  /**
   * Upsert a document using provided write settings.
   * @param id document id
   * @param document value of the document
   * @param upsertOptions Couchbase UpsertOptions
   * @tparam T type of the document
   * @return a future that completes when the upsert is done
   */
  def upsert[T: ClassTag](id: String, document: T, upsertOptions: UpsertOptions): Future[Done]

  /**
   * Upsert a document using given write settings and timeout
   * @param id document id
   * @param document document value to upsert
   * @param upsertOptions Couchbase UpsertOptions
   * @param timeout timeout for the operation
   * @return a future that completes after operation is done
   */
  def upsert[T: ClassTag](id: String, document: T, upsertOptions: UpsertOptions, timeout: FiniteDuration): Future[Done]

  /**
   * Replace a document using the default write settings.
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T: ClassTag](id: String, document: T): Future[Done]

  /**
   * Replace using the given replace options
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T: ClassTag](id: String, document: T, replaceOptions: ReplaceOptions): Future[Done]

  /**
   * Replace using write settings and timeout
   * @param id id of the document
   * @param document document value to replace
   * @param replaceOptions Couchbase replace options
   * @param timeout timeout for the operation
   * @return a future that completes after operation is done
   */
  def replace[T: ClassTag](id: String,
                           document: T,
                           replaceOptions: ReplaceOptions,
                           timeout: FiniteDuration): Future[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): Future[Done]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, removeOptions: RemoveOptions): Future[Done]

  /**
   * Removes document with given id, remove options and timeout
   * @param id id of the document to remove
   * @param removeOptions Couchbase remove options
   * @param timeout timeout
   * @return the id
   */
  def remove(id: String, removeOptions: RemoveOptions, timeout: FiniteDuration): Future[Done]

  /**
   * Create a secondary index for the current collection.
   *
   * @param indexName the name of the index.
   * @param createQueryIndexOptions Couchbase index options
   * @param fields the JSON fields to index
   * @return a [[scala.concurrent.Future]] of `true` if the index was/will be effectively created, `false`
   *      if the index existed and `ignoreIfExist` is `true`. Completion of the future does not guarantee the index is online
   *      and ready to be used.
   */
  def createIndex(indexName: String, createQueryIndexOptions: CreateQueryIndexOptions, fields: String*): Future[Done]

  /**
   * List the existing secondary indexes for the collection
   */
  def listIndexes(): Source[QueryIndex, NotUsed]
}
