/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlQuery, Statement}

/**
 * Scala API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {

  /**
   * Create a source query Couchbase by statement, emitted as [[com.couchbase.client.java.document.JsonDocument JsonDocument]]s.
   */
  def fromStatement(sessionSettings: CouchbaseSessionSettings,
                    statement: Statement,
                    bucketName: String): Source[JsonObject, NotUsed] =
    Source
      .setup { (materializer, _) =>
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
    Source
      .setup { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Source
          .fromFuture(session.map(_.streamedQuery(query))(materializer.system.dispatcher))
          .flatMapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

}
