/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.alpakka.couchbase._
import akka.stream.javadsl.Flow
import com.couchbase.client.java.document.{Document, JsonDocument}

/**
 * Java API: Factory methods for Couchbase flows.
 */
object CouchbaseFlow {

  /**
   * Create a flow to query Couchbase for by `id` and emit [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings, bucketName: String): Flow[String, JsonDocument, NotUsed] =
    scaladsl.CouchbaseFlow.fromId(sessionSettings, bucketName).asJava

  /**
   * Create a flow to query Couchbase for by `id` and emit documents of the given class.
   */
  def fromId[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                               bucketName: String,
                               target: Class[T]
  ): Flow[String, T, NotUsed] =
    scaladsl.CouchbaseFlow.fromId(sessionSettings, bucketName, target).asJava

  /**
   * Create a flow to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert(sessionSettings: CouchbaseSessionSettings,
             writeSettings: CouchbaseWriteSettings,
             bucketName: String
  ): Flow[JsonDocument, JsonDocument, NotUsed] =
    scaladsl.CouchbaseFlow.upsert(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to update or insert a Couchbase document of the given class.
   */
  def upsertDoc[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                                  writeSettings: CouchbaseWriteSettings,
                                  bucketName: String
  ): Flow[T, T, NotUsed] =
    scaladsl.CouchbaseFlow.upsertDoc(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertDocWithResult[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                                            writeSettings: CouchbaseWriteSettings,
                                            bucketName: String
  ): Flow[T, CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.upsertDocWithResult(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace(sessionSettings: CouchbaseSessionSettings,
              writeSettings: CouchbaseWriteSettings,
              bucketName: String
  ): Flow[JsonDocument, JsonDocument, NotUsed] =
    scaladsl.CouchbaseFlow.replace(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to replace a Couchbase document of the given class.
   */
  def replaceDoc[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                                   writeSettings: CouchbaseWriteSettings,
                                   bucketName: String
  ): Flow[T, T, NotUsed] =
    scaladsl.CouchbaseFlow.replaceDoc(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to replace a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def replaceDocWithResult[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                                             writeSettings: CouchbaseWriteSettings,
                                             bucketName: String
  ): Flow[T, CouchbaseWriteResult[T], NotUsed] =
    scaladsl.CouchbaseFlow.replaceDocWithResult(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             writeSettings: CouchbaseWriteSettings,
             bucketName: String
  ): Flow[String, String, NotUsed] =
    scaladsl.CouchbaseFlow.delete(sessionSettings, writeSettings, bucketName).asJava

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       writeSettings: CouchbaseWriteSettings,
                       bucketName: String
  ): Flow[String, CouchbaseDeleteResult, NotUsed] =
    scaladsl.CouchbaseFlow.deleteWithResult(sessionSettings, writeSettings, bucketName).asJava

}
