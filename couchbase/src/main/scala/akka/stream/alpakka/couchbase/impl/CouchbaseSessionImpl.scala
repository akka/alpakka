/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.javadsl
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query.{QueryOptions, QueryResult}
import com.couchbase.client.java.{AsyncBucket, AsyncCluster}

import scala.concurrent.Future
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps

/**
 * INTERNAL API
 *
 * @param cluster if provided, it will be shut down when `close()` is called
 */
@InternalApi
final private[couchbase] class CouchbaseSessionImpl(cluster: AsyncCluster, bucketName: String)
    extends CouchbaseSession {

  override def asJava: javadsl.CouchbaseSession = new CouchbaseSessionJavaAdapter(this)

  override def underlying: AsyncBucket = cluster.bucket(bucketName)

  override def streamedQuery(query: String): Source[JsonObject, NotUsed] = {
    Source.fromIterator(
      () =>
        cluster
          .query(query)
          .get()
          .rowsAsObject()
          .iterator()
          .asScala
    )
  }

  override def streamedQuery(query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed] =
    Source.fromIterator(
      () =>
        cluster
          .query(query, queryOptions)
          .get()
          .rowsAsObject()
          .iterator()
          .asScala
    )

  def close(): Future[Done] =
    Future.successful(Done)

  override def toString: String = s"CouchbaseSession(${underlying.name()})"

  private def getSingleResult(gr: QueryResult): Option[JsonObject] = {
    val rows = gr.rowsAsObject()
    if (rows.isEmpty) {
      return Option.empty
    }
    Option.apply(rows.iterator.next)
  }

  override def singleResponseQuery(query: String): Future[Option[JsonObject]] =
    cluster
      .query(query)
      .thenApply(getSingleResult(_))
      .asScala

  override def singleResponseQuery(query: String, queryOptions: QueryOptions): Future[Option[JsonObject]] =
    cluster
      .query(query, queryOptions)
      .thenApply(getSingleResult(_))
      .asScala

  override def cluster(): AsyncCluster = cluster
}
