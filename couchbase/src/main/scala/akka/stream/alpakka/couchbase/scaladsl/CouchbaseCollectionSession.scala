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
   * Insert a document using default InsertOptions and automatically detected Transcoder.
   * Will use RawBinaryTranscoder if document._2 is an array of bytes, RawStringTranscoder if its a String
   * and the default Transcoder otherwise.
   * @param document a tuple with string id and value of the document
   * @tparam T type of the document
   * @return a future that completes when the document was written
   */
  def insert[T](document: (String, T)): Future[(String, T)]

  /**
   * Insert a document using provided InsertOptions.
   * @param document a tuple with string id and value of the document
   * @param insertOptions Couchbase InsertOptions
   * @tparam T type of the document
   * @return a future that completes when the document was written or errors if the operation failed
   */
  def insert[T](document: (String, T), insertOptions: InsertOptions): Future[(String, T)]

  /**
   * Reads a document with given id as JsonObject
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getJsonObject(id: String): Future[(String, JsonObject)] =
    get(id, classOf[JsonObject])

  /**
   * Reads a document with given id as JsonArray
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getJsonArray(id: String): Future[(String, JsonArray)] = {
    get(id, classOf[JsonArray])
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
  def get[T](id: String, target: Class[T]): Future[(String, T)]

  /**
   * Reads a json document as JsonValue (either array or object)
   * @param id id of the document
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getDocument(id: String): Future[(String, JsonValue)]

  /**
   * Reads a document into an array of bytes
   * @param id Identifier of the document to fetch
   * @return a future that completes with Raw data for the document or errors if the operation failed
   */
  def getBytes(id: String): Future[(String, Array[Byte])]

  /**
   * Reads a document as JsonValue with given timeout
   * @param id Identifier of the document to fetch
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return a future that completes with the requested document or errors if the operation failed
   */
  def getDocument(id: String, timeout: FiniteDuration): Future[(String, JsonValue)]

  /**
   * Reads a document into an array of bytes with given timeout
   * @param id Identifier of the document to fetch
   * @param timeout fail the returned future with a TimeoutException if it takes longer than this
   * @return a future that completes with Raw data for the document or errors if the operation failed
   */
  def getBytes(id: String, timeout: FiniteDuration): Future[(String, Array[Byte])]

  /**
   * Upsert a document using the default write settings.
   * @param document a tuple that contains the id and value of the document
   * @tparam T type of the document
   * @return a future that completes when the upsert is done
   */
  def upsert[T](document: (String, T)): Future[(String, T)]

  /**
   * Upsert a document using provided write settings.
   * @param document a tuple that contains the id and value of the document
   * @param upsertOptions Couchbase UpsertOptions
   * @tparam T type of the document
   * @return a future that completes when the upsert is done
   */
  def upsert[T](document: (String, T), upsertOptions: UpsertOptions): Future[(String, T)]

  /**
   * Upsert a document using given write settings and timeout
   * @param document document id and value to upsert
   * @param upsertOptions Couchbase UpsertOptions
   * @param timeout timeout for the operation
   * @return the document id and value
   */
  def upsert[T](document: (String, T), upsertOptions: UpsertOptions, timeout: FiniteDuration): Future[(String, T)]

  /**
   * Replace a document using the default write settings.
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