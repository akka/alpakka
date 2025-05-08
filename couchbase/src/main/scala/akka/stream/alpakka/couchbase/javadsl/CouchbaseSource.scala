/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, scaladsl}
import akka.stream.javadsl.Source
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query.QueryOptions

/**
 * Java API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromQuery(sessionSettings: CouchbaseSessionSettings,
                bucketName: String, query: String): Source[JsonObject, NotUsed] =
    scaladsl.CouchbaseSource.fromQuery(sessionSettings, bucketName, query).asJava

  def fromQuery(sessionSettings: CouchbaseSessionSettings,
                bucketName: String, query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed] =
    scaladsl.CouchbaseSource.fromQuery(sessionSettings, bucketName, query, queryOptions).asJava
}
