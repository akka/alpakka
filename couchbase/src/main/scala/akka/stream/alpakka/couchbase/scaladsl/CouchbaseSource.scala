/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.impl.Setup
import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.query.{N1qlQuery, Statement}

/**
 * Scala API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {

  /**
   * Create a source to read a single document by `id` from Couchbase, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   *
   * In most use cases `CouchbaseSession.get(...)` methods might be simpler to use.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings, id: String, bucketName: String): Source[JsonDocument, NotUsed] =
    Source
      .single(id)
      .via(CouchbaseFlow.fromId(sessionSettings, bucketName))

  /**
   * Create a source to read a single document by `id` from Couchbase, emitted as document of the given class.
   *
   * In most use cases `CouchbaseSession.get(...)` methods might be simpler to use.
   */
  def fromId[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                               id: String,
                               bucketName: String,
                               target: Class[T]): Source[T, NotUsed] =
    Source
      .single(id)
      .via(CouchbaseFlow.fromId(sessionSettings, bucketName, target))

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromStatement(sessionSettings: CouchbaseSessionSettings,
                    statement: Statement,
                    bucketName: String): Source[JsonObject, NotUsed] =
    Setup
      .source { materializer => _ =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Source
          .fromFuture(session.map(_.streamedQuery(statement))(materializer.system.dispatcher))
          .flatMapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromN1qlQuery(sessionSettings: CouchbaseSessionSettings,
                    query: N1qlQuery,
                    bucketName: String): Source[JsonObject, NotUsed] =
    Setup
      .source { materializer => _ =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Source
          .fromFuture(session.map(_.streamedQuery(query))(materializer.system.dispatcher))
          .flatMapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

}
