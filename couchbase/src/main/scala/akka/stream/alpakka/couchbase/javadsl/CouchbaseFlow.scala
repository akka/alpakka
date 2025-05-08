/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.alpakka.couchbase._
import akka.stream.javadsl.Flow
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv.{RemoveOptions, ReplaceOptions, UpsertOptions}

/**
 * Java API: Factory methods for Couchbase flows.
 */
object CouchbaseFlow {

  /**
   * Create a flow to query Couchbase for by `id` and emit [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings, bucketName: String, scopeName: String, collectionName: String): Flow[String, (String, JsonValue), NotUsed] =
    scaladsl.CouchbaseFlow.fromId(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to query Couchbase for by `id` and emit documents of the given class.
   */
  def fromId[T](sessionSettings: CouchbaseSessionSettings,
                               bucketName: String, scopeName: String, collectionName: String,
                               target: Class[T]): Flow[String, (String, T), NotUsed] =
    scaladsl.CouchbaseFlow.fromId(sessionSettings, bucketName, scopeName, collectionName, target).asJava

  /**
   * Create a flow to query Couchbase for by `id` and emit [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def bytesFromId(sessionSettings: CouchbaseSessionSettings, bucketName: String, scopeName: String, collectionName: String): Flow[String, (String, Array[Byte]), NotUsed] =
    scaladsl.CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert[T](sessionSettings: CouchbaseSessionSettings,
             bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), (String, T), NotUsed] =
    scaladsl.CouchbaseFlow.upsert(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert[T](sessionSettings: CouchbaseSessionSettings,
             upsertOptions: UpsertOptions,
             bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), (String, T), NotUsed] =
    scaladsl.CouchbaseFlow.upsert(sessionSettings, upsertOptions, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T](sessionSettings: CouchbaseSessionSettings,
                          bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.upsertWithResult[T](sessionSettings, bucketName, scopeName, collectionName).asJava[(String, T)]
  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T](sessionSettings: CouchbaseSessionSettings,
                             upsertOptions: UpsertOptions,
                             bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.upsertWithResult[T](sessionSettings, upsertOptions, bucketName, scopeName, collectionName).asJava[(String, T)]

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T](sessionSettings: CouchbaseSessionSettings,
                 bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.replaceWithResult[T](sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T](sessionSettings: CouchbaseSessionSettings,
                 replaceOptions: ReplaceOptions,
                 bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.replaceWithResult[T](sessionSettings, replaceOptions, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T](sessionSettings: CouchbaseSessionSettings,
                 bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), (String, T), NotUsed] =
    scaladsl.CouchbaseFlow.replace(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T](sessionSettings: CouchbaseSessionSettings,
              replaceOptions: ReplaceOptions,
              bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), (String, T), NotUsed] =
    scaladsl.CouchbaseFlow.replace(sessionSettings, replaceOptions, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             bucketName: String, scopeName: String, collectionName: String): Flow[String, String, NotUsed] =
    scaladsl.CouchbaseFlow.delete(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             removeOptions: RemoveOptions,
             bucketName: String, scopeName: String, collectionName: String): Flow[String, String, NotUsed] =
    scaladsl.CouchbaseFlow.delete(sessionSettings, removeOptions, bucketName, scopeName, collectionName).asJava


  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    scaladsl.CouchbaseFlow.deleteWithResult(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       removeOptions: RemoveOptions,
                       bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    scaladsl.CouchbaseFlow.deleteWithResult(sessionSettings, removeOptions, bucketName, scopeName, collectionName).asJava

}
