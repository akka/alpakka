/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.annotation.DoNotInherit
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.impl.CouchbaseSessionJavaAdapter
import akka.stream.alpakka.couchbase.scaladsl.{CouchbaseSession => ScalaDslCouchbaseSession}
import akka.stream.javadsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query.QueryOptions
import com.couchbase.client.java.{AsyncBucket, AsyncCluster}

import java.util.Optional
import java.util.concurrent.{CompletionStage, Executor}
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._

/**
 * Java API: Gives access to Couchbase.
 *
 * @see [[akka.stream.alpakka.couchbase.CouchbaseSessionRegistry]]
 */
object CouchbaseSession {

  /**
   * Create a session against the given bucket. The couchbase client used to connect will be created and then closed when
   * the session is closed.
   */
  def create(settings: CouchbaseSessionSettings,
             bucketName: String,
             executor: Executor): CompletionStage[CouchbaseSession] =
    ScalaDslCouchbaseSession
      .apply(settings, bucketName)(executionContext(executor))
      .map(new CouchbaseSessionJavaAdapter(_): CouchbaseSession)(
        ExecutionContext.parasitic
      )
      .asJava

  /**
   * Create a given bucket using a pre-existing cluster client, allowing for it to be shared among
   * multiple `CouchbaseSession`s. The cluster client's life-cycle is the user's responsibility.
   */
  def create(client: AsyncCluster, bucketName: String): CompletionStage[CouchbaseSession] =
    ScalaDslCouchbaseSession(client, bucketName)
      .map(new CouchbaseSessionJavaAdapter(_): CouchbaseSession)(
        ExecutionContext.parasitic
      )
      .asJava

  /**
   * Connects to a Couchbase cluster by creating an `AsyncCluster`.
   * The life-cycle of it is the user's responsibility.
   */
  def createClient(settings: CouchbaseSessionSettings, executor: Executor): CompletionStage[AsyncCluster] =
    ScalaDslCouchbaseSession
      .createClusterClient(settings)(executionContext(executor))
      .asJava

  private def executionContext(executor: Executor): ExecutionContext =
    executor match {
      case ec: ExecutionContext => ec
      case _ => ExecutionContext.fromExecutor(executor)
    }
}

/**
 * Java API: A Couchbase session allowing querying and interacting with a specific couchbase bucket.
 *
 * Not for user extension.
 */
// must be an abstract class, otherwise static forwarders are missing for companion object if building with Scala 2.11
@DoNotInherit
abstract class CouchbaseSession {

  def underlying: AsyncBucket

  def asScala: ScalaDslCouchbaseSession

  def streamedQuery(query: String): Source[JsonObject, NotUsed]
  def streamedQuery(query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed]
  def singleResponseQuery(query: String): CompletionStage[Optional[JsonObject]]
  def singleResponseQuery(query: String, queryOptions: QueryOptions): CompletionStage[Optional[JsonObject]]

  def collection(scopeName: String, collectionName: String): CouchbaseCollectionSession

  /**
   * Close the session and release all resources it holds. Subsequent calls to other methods will likely fail.
   */
  def close(): CompletionStage[Done]

}
