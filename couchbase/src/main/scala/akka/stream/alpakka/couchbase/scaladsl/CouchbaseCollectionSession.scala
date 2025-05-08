package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.annotation.DoNotInherit
import akka.stream.alpakka.couchbase.impl.CouchbaseCollectionSessionImpl
import akka.stream.scaladsl.Source
import com.couchbase.client.java.json.{JsonArray, JsonObject, JsonValue}
import com.couchbase.client.java.kv.{GetOptions, InsertOptions, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.java.manager.query.{CreateQueryIndexOptions, QueryIndex}
import com.couchbase.client.java.{AsyncCollection, AsyncScope}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
   * Insert a JSON document using the default write settings
   * @param document A tuple where first element is id of the document and second is its value
   * @return A Future that completes with the id of the written document when the write is done
   */
  def insert[T](document: (String, T)): Future[(String, T)]
  def insert[T](document: (String, T), insertOptions: InsertOptions): Future[(String, T)]

  def getJsonObject(id: String): Future[(String, JsonObject)] =
    get(id, classOf[JsonObject])

  def getJsonArray(id: String): Future[(String, JsonArray)] = {
    get(id, classOf[JsonArray])
  }

  def get[T](id: String, target: Class[T]): Future[(String, T)]
  /**
   * @return A document if found or none if there is no document for the id
   */
  def getDocument(id: String): Future[(String, JsonValue)]

  /**
   * @param id Identifier of the document to fetch
   * @return Raw data for the document or none
   */
  def getBytes(id: String): Future[(String, Array[Byte])]

  /**
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return A document if found or none if there is no document for the id
   */
  def getDocument(id: String, timeout: FiniteDuration): Future[(String, JsonValue)]

  /**
   * @return A raw document data if found or none if there is no document for the id
   */
  def getBytes(id: String, timeout: FiniteDuration): Future[(String, Array[Byte])]

  /**
   * Upsert using the default write settings.
   * @return a future that completes when the upsert is done
   */
  def upsert[T](document: (String, T)): Future[(String, T)]

  /**
   * Upsert using the given write settings
   *
   * Separate from `upsert` to make the most common case smoother with the type inference
   *
   * @return a future that completes when the upsert is done
   */
  def upsert[T](document: (String, T), upsertOptions: UpsertOptions): Future[(String, T)]

  /**
   * Upsert using given write settings and timeout
   * @param document document id and value to upsert
   * @param upsertOptions Couchbase UpsertOptions
   * @param timeout timeout for the operation
   * @return the document id and value
   */
  def upsert[T](document: (String, T), upsertOptions: UpsertOptions, timeout: FiniteDuration): Future[(String, T)]

  /**
   * Replace using the default write settings.
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T](document: (String, T)): Future[(String, T)]

  /**
   * Replace using the given replace options
   *
   * For replacing other types of documents see `replaceDoc`.
   *
   * @return a future that completes when the replace is done
   */
  def replace[T](document: (String, T), replaceOptions: ReplaceOptions): Future[(String, T)]

  /**
   * Replace using write settings and timeout
   * @param document document id and value to replace
   * @param replaceOptions Couchbase replace options
   * @param timeout timeout for the operation
   * @return the document id and value
   */
  def replace[T](document: (String, T), replaceOptions: ReplaceOptions, timeout: FiniteDuration): Future[(String, T)]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String): Future[String]

  /**
   * Remove a document by id using the default write settings.
   *
   * @return Future that completes when the document has been removed, if there is no such document
   *         the future is failed with a `DocumentDoesNotExistException`
   */
  def remove(id: String, removeOptions: RemoveOptions): Future[String]

  /**
   * Removes document with given id, remove options and timeout
   * @param id id of the document to remove
   * @param removeOptions Couchbase remove options
   * @param timeout timeout
   * @return the id
   */
  def remove(id: String, removeOptions: RemoveOptions, timeout: FiniteDuration): Future[String]

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
  def createIndex(indexName: String, createQueryIndexOptions: CreateQueryIndexOptions, fields: String*): Future[Void]

  /**
   * List the existing secondary indexes for the collection
   */
  def listIndexes(): Source[QueryIndex, NotUsed]
}