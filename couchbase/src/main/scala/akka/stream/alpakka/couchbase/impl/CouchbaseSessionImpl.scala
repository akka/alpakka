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
import rx.RxReactiveStreams

import scala.concurrent.{ExecutionContext, Future}
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
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        rx.Observable.from(
          cluster
            .query(query)
            .get()
            .rowsAsObject()
        )
      )
    )
  }

  override def streamedQuery(query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed] = {
    Source.fromPublisher(
      RxReactiveStreams.toPublisher(
        rx.Observable.from(
          cluster
            .query(query, queryOptions)
            .get()
            .rowsAsObject()
        )
      )
    )
  }

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

  /**
   * Executes a query and returns its first result, discarding any other results
   * Tip: use `LIMIT 1` in your query to avoid fetching more than 1 result
   * @param query — the query to be executed
   * @return the first row of the resultset
   */
  override def singleResponseQuery(query: String): Future[Option[JsonObject]] =
    cluster
      .query(query)
      .asScala
      .map(getSingleResult)(ExecutionContext.parasitic)

  /**
   * Executes a query and returns its first result, discarding any other results
   * Tip: use `LIMIT 1` in your query to avoid fetching more than 1 result
   * @param query the query to be executed
   * @param queryOptions Couchbase SDK QueryOptions object
   * @return the first row of the resultset
   */
  override def singleResponseQuery(query: String, queryOptions: QueryOptions): Future[Option[JsonObject]] =
    cluster
      .query(query, queryOptions)
      .asScala
      .map(getSingleResult)(ExecutionContext.parasitic)

  override def cluster(): AsyncCluster = cluster
}
