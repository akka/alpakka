/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.annotation.{DoNotInherit, InternalApi}
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.impl.{CouchbaseCollectionSessionImpl, CouchbaseSessionImpl}
import akka.stream.alpakka.couchbase.javadsl.{CouchbaseSession => JavaDslCouchbaseSession}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java._
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query._

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Scala API: Gives access to Couchbase.
 *
 * @see [[akka.stream.alpakka.couchbase.CouchbaseSessionRegistry]]
 */
object CouchbaseSession {

  /**
   * Create a session against the given bucket. The couchbase client used to connect will be created and then closed when
   * the session is closed.
   */
  def apply(settings: CouchbaseSessionSettings, bucketName: String)(implicit ec: ExecutionContext): Future[CouchbaseSession] =
    createClusterClient(settings)
      .flatMap(c => apply(c, bucketName))

  /**
   * Create a given bucket using a pre-existing cluster client, allowing for it to be shared among
   * multiple `CouchbaseSession`s. The cluster client's life-cycle is the user's responsibility.
   */
  def apply(cluster: AsyncCluster, bucketName: String): Future[CouchbaseSession] =
    Future.successful(new CouchbaseSessionImpl(cluster, bucketName))


  /**
   * INTERNAL API.
   *
   * Connects to a Couchbase cluster by creating an `AsyncCluster`.
   * The life-cycle of it is the user's responsibility.
   */
  @InternalApi
  private[couchbase] def createClusterClient(
      settings: CouchbaseSessionSettings
  )(implicit ec: ExecutionContext): Future[AsyncCluster] =
    settings.enriched
      .flatMap { enrichedSettings =>
        val clusterOptions = ClusterOptions.clusterOptions(
          enrichedSettings.username, enrichedSettings.password
        )
        Future(enrichedSettings.environment match {
          case Some(environment) =>
            AsyncCluster.connect(
              enrichedSettings.nodes.mkString(","),
              clusterOptions.environment(environment)
            )
          case None =>
            AsyncCluster.connect(
              enrichedSettings.nodes.mkString(","),
              clusterOptions
            )
        })
      }

}

/**
 * Scala API: A Couchbase session allowing querying and interacting with a specific couchbase bucket.
 *
 * Not for user extension.
 */
@DoNotInherit
trait CouchbaseSession {

  def underlying: AsyncBucket

  def asJava: JavaDslCouchbaseSession

  private val collectionSessions = new AtomicReference(mutable.WeakHashMap.empty[(String, String), CouchbaseCollectionSession])

  def streamedQuery(query: String): Source[JsonObject, NotUsed]
  def streamedQuery(query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed]
  def singleResponseQuery(query: String): Future[Option[JsonObject]]
  def singleResponseQuery(query: String, queryOptions: QueryOptions): Future[Option[JsonObject]]

  def collection(scopeName: String, collectionName: String): CouchbaseCollectionSession = {
    collectionSessions.get.get((scopeName, collectionName)) match {
      case Some(session) => session
      case _ => {
        val oldSessions = collectionSessions.get()
        val newSession = new CouchbaseCollectionSessionImpl(this, scopeName, collectionName)
        val newSessions = oldSessions.clone().addOne(((scopeName, collectionName), newSession))
        if (collectionSessions.compareAndSet(oldSessions, newSessions)) {
          newSession
        } else {
          collection(scopeName, collectionName)
        }
      }
    }
  }

  /**
   * Close the session and release all resources it holds. Subsequent calls to other methods will likely fail.
   */
  def close(): Future[Done]
}

