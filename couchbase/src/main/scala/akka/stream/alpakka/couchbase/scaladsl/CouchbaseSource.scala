/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.NotUsed
import akka.stream.alpakka.couchbase.{CouchbaseSessionRegistry, CouchbaseSessionSettings}
import akka.stream.scaladsl.Source
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query.QueryOptions

/**
 * Scala API: Factory methods for Couchbase sources.
 */
object CouchbaseSource {
  def fromQuery(sessionSettings: CouchbaseSessionSettings,
                bucketName: String,
                query: String,
                queryOptions: QueryOptions = QueryOptions.queryOptions()): Source[JsonObject, NotUsed] =
    Source
      .fromMaterializer { (materializer, _) =>
        Source
          .future(
            CouchbaseSessionRegistry(materializer.system)
              .sessionFor(sessionSettings, bucketName)
              .map(_.streamedQuery(query, queryOptions))(materializer.system.dispatcher)
          )
          .flatMapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
}
