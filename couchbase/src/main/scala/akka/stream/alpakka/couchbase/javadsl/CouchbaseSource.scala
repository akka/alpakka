/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.{scaladsl, CouchbaseSessionSettings}
import akka.stream.javadsl.Source
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}

/**
 * Java API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {

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
