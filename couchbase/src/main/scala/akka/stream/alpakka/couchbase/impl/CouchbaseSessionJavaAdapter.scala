/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.javadsl.CouchbaseCollectionSession
import akka.stream.alpakka.couchbase.{javadsl, scaladsl}
import akka.stream.javadsl.Source
import akka.{Done, NotUsed}
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.query.QueryOptions

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] final class CouchbaseSessionJavaAdapter(delegate: scaladsl.CouchbaseSession)
    extends javadsl.CouchbaseSession {

  override def asScala: scaladsl.CouchbaseSession = delegate

  override def underlying: AsyncBucket = delegate.underlying

  override def close(): CompletionStage[Done] = delegate.close().asJava

  override def streamedQuery(query: String): Source[JsonObject, NotUsed] =
    delegate.streamedQuery(query).asJava

  override def streamedQuery(query: String, queryOptions: QueryOptions): Source[JsonObject, NotUsed] =
    delegate.streamedQuery(query, queryOptions).asJava

  override def singleResponseQuery(query: String): CompletionStage[Optional[JsonObject]] =
    delegate.singleResponseQuery(query).map(_.toJava).asJava

  override def singleResponseQuery(query: String, queryOptions: QueryOptions): CompletionStage[Optional[JsonObject]] =
    delegate.singleResponseQuery(query, queryOptions).map(_.toJava).asJava

  override def collection(scopeName: String, collectionName: String): CouchbaseCollectionSession =
    delegate.collection(scopeName, collectionName).asJava
}
