/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase._
import akka.stream.scaladsl.{Keep, Sink}
import com.couchbase.client.java.document.{Document, JsonDocument}

import scala.concurrent.Future

/**
 * Scala API: Factory methods for Couchbase sinks.
 */
object CouchbaseSink {

  /**
   * Create a sink to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert(sessionSettings: CouchbaseSessionSettings,
             writeSettings: CouchbaseWriteSettings,
             bucketName: String): Sink[JsonDocument, Future[Done]] =
    CouchbaseFlow.upsert(sessionSettings, writeSettings, bucketName).toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to update or insert a Couchbase document of the given class.
   */
  def upsertDoc[T <: Document[_]](sessionSettings: CouchbaseSessionSettings,
                                  writeSettings: CouchbaseWriteSettings,
                                  bucketName: String): Sink[T, Future[Done]] =
    CouchbaseFlow
      .upsertDoc(sessionSettings, writeSettings, bucketName)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Create a sink to delete documents from Couchbase by `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             writeSettings: CouchbaseWriteSettings,
             bucketName: String): Sink[String, Future[Done]] =
    CouchbaseFlow.delete(sessionSettings, writeSettings, bucketName).toMat(Sink.ignore)(Keep.right)

}
