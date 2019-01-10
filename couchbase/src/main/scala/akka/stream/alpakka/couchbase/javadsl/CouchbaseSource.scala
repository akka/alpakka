/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.{scaladsl, CouchbaseSessionSettings}
import akka.stream.javadsl.Source
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.query.{N1qlQuery, Statement}

/**
 * Java API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {

  /**
   * Create a source to read a single document by `id` from Couchbase, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   *
   * In most use cases `CouchbaseSession.get(...)` methods might be simpler to use.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings, id: String, bucketName: String): Source[JsonDocument, NotUsed] =
    scaladsl.CouchbaseSource.fromId(sessionSettings, id, bucketName).asJava

  /**
   * Create a source to read a single document by `id` from Couchbase, emitted as document of the given class.
   *
   * In most use cases `CouchbaseSession.get(...)` methods might be simpler to use.
   */
  def fromId[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                               id: String,
                               bucketName: String,
                               target: Class[T]): Source[T, NotUsed] =
    scaladsl.CouchbaseSource.fromId(sessionSettings, id, bucketName, target).asJava

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromStatement(sessionSettings: CouchbaseSessionSettings,
                    statement: Statement,
                    bucketName: String): Source[JsonObject, NotUsed] =
    scaladsl.CouchbaseSource.fromStatement(sessionSettings, statement, bucketName).asJava

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromN1qlQuery(sessionSettings: CouchbaseSessionSettings,
                    query: N1qlQuery,
                    bucketName: String): Source[JsonObject, NotUsed] =
    scaladsl.CouchbaseSource.fromN1qlQuery(sessionSettings, query, bucketName).asJava

}
