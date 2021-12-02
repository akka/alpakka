/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
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
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Source
          .future(session.map(_.streamedQuery(statement))(materializer.system.dispatcher))
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
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Source
          .future(session.map(_.streamedQuery(query))(materializer.system.dispatcher))
          .flatMapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)

}
