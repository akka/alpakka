/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.stream.alpakka.couchbase._
import akka.stream.javadsl.{Keep, Sink}
import akka.{Done, NotUsed}
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv.{RemoveOptions, ReplaceOptions, UpsertOptions}

import java.util.concurrent.CompletionStage

/**
 * Java API: Factory methods for Couchbase sinks.
 */
object CouchbaseSink {

  /**
   * Create a sink to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert(sessionSettings: CouchbaseSessionSettings,
             bucketName: String,
             scopeName: String,
             collectionName: String): Sink[CouchbaseDocument[JsonValue], CompletionStage[Done]] =
    CouchbaseFlow
      .upsert(sessionSettings, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a sink to update or insert a Couchbase document.
   */
  def upsert(sessionSettings: CouchbaseSessionSettings,
             upsertOptions: UpsertOptions,
             bucketName: String,
             scopeName: String,
             collectionName: String): Sink[CouchbaseDocument[JsonValue], CompletionStage[Done]] =
    CouchbaseFlow
      .upsert(sessionSettings, upsertOptions, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a sink to replace a Couchbase document.
   */
  def replace(sessionSettings: CouchbaseSessionSettings,
              bucketName: String,
              scopeName: String,
              collectionName: String): Sink[CouchbaseDocument[Any], CompletionStage[Done]] =
    CouchbaseFlow
      .replace(sessionSettings, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a sink to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace(sessionSettings: CouchbaseSessionSettings,
              replaceOptions: ReplaceOptions,
              bucketName: String,
              scopeName: String,
              collectionName: String): Sink[(String, Any), CompletionStage[Done]] =
    CouchbaseFlow
      .replace(sessionSettings, replaceOptions, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a sink to delete documents from Couchbase by `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             bucketName: String,
             scopeName: String,
             collectionName: String): Sink[String, CompletionStage[Done]] =
    CouchbaseFlow
      .delete(sessionSettings, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Create a sink to delete documents from Couchbase by `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             removeOptions: RemoveOptions,
             bucketName: String,
             scopeName: String,
             collectionName: String): Sink[String, CompletionStage[Done]] =
    CouchbaseFlow
      .delete(sessionSettings, removeOptions, bucketName, scopeName, collectionName)
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

}
