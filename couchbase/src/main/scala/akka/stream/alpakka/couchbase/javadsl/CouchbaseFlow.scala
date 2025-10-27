/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.couchbase._
import akka.stream.javadsl.Flow
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv.{RemoveOptions, ReplaceOptions, UpsertOptions}

import scala.reflect.ClassTag

/**
 * Java API: Factory methods for Couchbase flows.
 */
object CouchbaseFlow {

  /**
   * Create a flow to query Couchbase for by `id` and emit [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings,
             bucketName: String,
             scopeName: String,
             collectionName: String): Flow[String, CouchbaseDocument[JsonValue], NotUsed] =
    scaladsl.CouchbaseFlow.fromId[JsonValue](sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to query Couchbase for by `id` and emit documents of the given class.
   */
  def fromId[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                          bucketName: String,
                          scopeName: String,
                          collectionName: String): Flow[String, CouchbaseDocument[T], NotUsed] =
    scaladsl.CouchbaseFlow.fromId[T](sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to query Couchbase for by `id` and emit [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def bytesFromId(sessionSettings: CouchbaseSessionSettings,
                  bucketName: String,
                  scopeName: String,
                  collectionName: String): Flow[String, CouchbaseDocument[Array[Byte]], NotUsed] =
    scaladsl.CouchbaseFlow.bytesFromId(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to update or insert a Couchbase document.
   */
  def upsert[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                          bucketName: String,
                          scopeName: String,
                          collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    scaladsl.CouchbaseFlow
      .upsert[T](sessionSettings, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to update or insert a Couchbase document.
   */
  def upsert[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                          upsertOptions: UpsertOptions,
                          bucketName: String,
                          scopeName: String,
                          collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    scaladsl.CouchbaseFlow
      .upsert[T](sessionSettings, upsertOptions, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                                    bucketName: String,
                                    scopeName: String,
                                    collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    scaladsl.CouchbaseFlow
      .upsertWithResult[T](sessionSettings, bucketName, scopeName, collectionName)
      .asJava[CouchbaseDocument[T]]

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                                    upsertOptions: UpsertOptions,
                                    bucketName: String,
                                    scopeName: String,
                                    collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    scaladsl.CouchbaseFlow
      .upsertWithResult[T](sessionSettings, upsertOptions, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T: ClassTag](
      sessionSettings: CouchbaseSessionSettings,
      bucketName: String,
      scopeName: String,
      collectionName: String
  ): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    scaladsl.CouchbaseFlow.replaceWithResult[T](sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T: ClassTag](
      sessionSettings: CouchbaseSessionSettings,
      replaceOptions: ReplaceOptions,
      bucketName: String,
      scopeName: String,
      collectionName: String
  ): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    scaladsl.CouchbaseFlow
      .replaceWithResult[T](sessionSettings, replaceOptions, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                           bucketName: String,
                           scopeName: String,
                           collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    scaladsl.CouchbaseFlow
      .replace[T](sessionSettings, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                           replaceOptions: ReplaceOptions,
                           bucketName: String,
                           scopeName: String,
                           collectionName: String): Flow[(String, T), Done, NotUsed] =
    scaladsl.CouchbaseFlow
      .replace[T](sessionSettings, replaceOptions, bucketName, scopeName, collectionName)
      .asJava

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             bucketName: String,
             scopeName: String,
             collectionName: String): Flow[String, String, NotUsed] =
    scaladsl.CouchbaseFlow.delete(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             removeOptions: RemoveOptions,
             bucketName: String,
             scopeName: String,
             collectionName: String): Flow[String, String, NotUsed] =
    scaladsl.CouchbaseFlow.delete(sessionSettings, removeOptions, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       bucketName: String,
                       scopeName: String,
                       collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    scaladsl.CouchbaseFlow.deleteWithResult(sessionSettings, bucketName, scopeName, collectionName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       removeOptions: RemoveOptions,
                       bucketName: String,
                       scopeName: String,
                       collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    scaladsl.CouchbaseFlow
      .deleteWithResult(sessionSettings, removeOptions, bucketName, scopeName, collectionName)
      .asJava

}
