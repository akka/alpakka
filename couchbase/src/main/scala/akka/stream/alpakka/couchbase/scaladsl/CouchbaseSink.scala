/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase._
import akka.stream.scaladsl.{Keep, Sink}
import com.couchbase.client.java.kv.{RemoveOptions, UpsertOptions}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Scala API: Factory methods for Couchbase sinks.
 */
object CouchbaseSink {

  /**
   * Create a sink to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert[T: ClassTag](sessionSettings: CouchbaseSessionSettings,
                          upsertOptions: UpsertOptions,
                          bucketName: String,
                          scopeName: String,
                          collectionName: String): Sink[CouchbaseDocument[T], Future[Done]] =
    CouchbaseFlow
      .upsert[T](sessionSettings, upsertOptions, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to delete documents from Couchbase by `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             removeOptions: RemoveOptions,
             bucketName: String,
             scopeName: String,
             collectionName: String): Sink[String, Future[Done]] =
    CouchbaseFlow
      .delete(sessionSettings, removeOptions, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore)(Keep.right)

}
