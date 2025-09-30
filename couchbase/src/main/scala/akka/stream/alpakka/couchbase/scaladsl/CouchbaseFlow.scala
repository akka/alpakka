/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl
import akka.stream.ActorAttributes
import akka.stream.Supervision.Resume
import akka.stream.alpakka.couchbase._
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.couchbase.client.java.codec.{RawBinaryTranscoder, RawStringTranscoder}
import com.couchbase.client.java.json.JsonValue
import com.couchbase.client.java.kv.{RemoveOptions, ReplaceOptions, UpsertOptions}


/**
 * Scala API: Factory methods for Couchbase flows.
 */
object CouchbaseFlow {

  /**
   * Create a flow to query Couchbase for by `id` and
   * emit CouchbaseDocument[Byte]s.
   */
  def bytesFromId(sessionSettings: CouchbaseSessionSettings, bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDocument[Array[Byte]], NotUsed] =

    Flow
      .fromMaterializer { (materializer, _) =>
        implicit val ec = materializer.system.dispatcher
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(id => session
            .map(_.collection(scopeName, collectionName))
            .flatMap(_.getBytes(id))
          )
          .withAttributes(ActorAttributes.supervisionStrategy(_ => Resume))
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to query Couchbase for by `id` and
   * emit CouchbaseDocument[JsonValue]s.
   */
  def fromId(sessionSettings: CouchbaseSessionSettings, bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDocument[JsonValue], NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        implicit val ec = materializer.system.dispatcher
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(id => session
            .map(_.collection(scopeName, collectionName))
            .flatMap(_.getDocument(id))
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to query Couchbase for by `id` and emit documents of the given class.
   */
  def fromId[T](sessionSettings: CouchbaseSessionSettings,
                               bucketName: String, scopeName: String, collectionName: String,
                               target: Class[T]): Flow[String, CouchbaseDocument[T], NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        implicit val ec = materializer.system.dispatcher
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(id => session
            .map(_.collection(scopeName, collectionName))
            .flatMap(_.get(id /* timeout? */, target)))
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert[T](sessionSettings: CouchbaseSessionSettings,
                bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        implicit val ec = materializer.system.dispatcher
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        val collection = session.map(_.collection(scopeName, collectionName))
        Flow.futureFlow[CouchbaseDocument[T], Done, NotUsed] {
          collection.map { c =>
            Flow[CouchbaseDocument[T]]
              .mapAsync(sessionSettings.parallelism) { doc =>
                c.upsert(doc.getId, doc.getDocument)
              }
          }
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to update or insert a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def upsert[T](sessionSettings: CouchbaseSessionSettings,
             upsertOptions: UpsertOptions,
             bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        implicit val ec = materializer.system.dispatcher
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        val collection = session.map(_.collection(scopeName, collectionName))
        Flow.futureFlow[CouchbaseDocument[T], Done, NotUsed] {
          collection.map{ c =>
            Flow[CouchbaseDocument[T]]
              .mapAsync(sessionSettings.parallelism) { doc =>
                c.upsert(doc.getId, doc.getDocument)
              }
          }
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T](sessionSettings: CouchbaseSessionSettings,
                          bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[CouchbaseDocument[T]]
          .mapAsync(sessionSettings.parallelism)(
            doc => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.upsert(doc.getId, doc.getDocument))
                .map(_ => CouchbaseWriteSuccess(doc.getId))
                .recover(CouchbaseWriteFailure(doc.getId, _))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to update or insert a Couchbase document of the given class and emit a result so that write failures
   * can be handled in-stream.
   */
  def upsertWithResult[T](sessionSettings: CouchbaseSessionSettings,
                          upsertOptions: UpsertOptions,
                          bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[CouchbaseDocument[T]]
          .mapAsync(sessionSettings.parallelism)(
            doc => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.upsert(doc.getId, doc.getDocument, upsertOptions))
                .map(_ => CouchbaseWriteSuccess(doc.getId))
                .recover(ex => CouchbaseWriteFailure(doc.getId, ex.getCause))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T](sessionSettings: CouchbaseSessionSettings,
                 bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        implicit val executor = materializer.system.dispatcher
        Flow[CouchbaseDocument[T]]
          .mapAsync(sessionSettings.parallelism)(
            doc => session
              .map(_.collection(scopeName, collectionName))
              .flatMap(_.replace(doc.getId, doc.getDocument))
              .map(_ => CouchbaseWriteSuccess(doc.getId))
              .recover(ex => CouchbaseWriteFailure(doc.getId, ex.getCause))
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replaceWithResult[T](sessionSettings: CouchbaseSessionSettings,
                 replaceOptions: ReplaceOptions,
                 bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], CouchbaseWriteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        implicit val executor = materializer.system.dispatcher
        Flow[CouchbaseDocument[T]]
          .mapAsync(sessionSettings.parallelism)(
            doc => session
              .map(_.collection(scopeName, collectionName))
              .flatMap(_.replace(doc.getId, doc.getDocument, replaceOptions))
              .map(_ => CouchbaseWriteSuccess(doc.getId))
              .recover(ex => CouchbaseWriteFailure(doc.getId, ex.getCause))
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T](sessionSettings: CouchbaseSessionSettings,
                 bucketName: String, scopeName: String, collectionName: String): Flow[CouchbaseDocument[T], Done, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        implicit val executor = materializer.system.dispatcher
        Flow[CouchbaseDocument[T]]
          .mapAsync(sessionSettings.parallelism)(
            doc => {
              val op = session
                .map(_.collection(scopeName, collectionName))

              if (doc.getDocument.isInstanceOf[Array[Byte]])
                op.flatMap(_.replace(doc.getId, doc.getDocument, ReplaceOptions.replaceOptions().transcoder(RawBinaryTranscoder.INSTANCE)))
              else if (doc.getDocument.isInstanceOf[String])
                op.flatMap(_.replace(doc.getId, doc.getDocument, ReplaceOptions.replaceOptions().transcoder(RawStringTranscoder.INSTANCE)))
              else
                op.flatMap(_.replace(doc.getId, doc.getDocument))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to replace a Couchbase [[com.couchbase.client.java.document.JsonDocument JsonDocument]].
   */
  def replace[T](sessionSettings: CouchbaseSessionSettings,
              replaceOptions: ReplaceOptions,
              bucketName: String, scopeName: String, collectionName: String): Flow[(String, T), Done, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        implicit val executor = materializer.system.dispatcher
        Flow[(String, T)]
          .mapAsync(sessionSettings.parallelism)(
            doc => session
              .map(_.collection(scopeName, collectionName))
              .flatMap(_.replace(doc._1, doc._2, replaceOptions))
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             bucketName: String, scopeName: String, collectionName: String): Flow[String, String, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(
            id => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.remove(id))
                .map(_ => id)
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to delete documents from Couchbase by `id`. Emits the same `id`.
   */
  def delete(sessionSettings: CouchbaseSessionSettings,
             removeOptions: RemoveOptions,
             bucketName: String, scopeName: String, collectionName: String): Flow[String, String, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(
            id => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.remove(id, removeOptions))
                .map(_ => id)
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(
            id => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.remove(id))
                .map(_ => CouchbaseDeleteSuccess(id))
                .recover(ex => CouchbaseDeleteFailure(id, ex.getCause))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Create a flow to delete documents from Couchbase by `id` and emit operation outcome containing the same `id`.
   */
  def deleteWithResult(sessionSettings: CouchbaseSessionSettings,
                       removeOptions: RemoveOptions,
                       bucketName: String, scopeName: String, collectionName: String): Flow[String, CouchbaseDeleteResult, NotUsed] =
    Flow
      .fromMaterializer { (materializer, _) =>
        val session = CouchbaseSessionRegistry(materializer.system).sessionFor(sessionSettings, bucketName)
        Flow[String]
          .mapAsync(sessionSettings.parallelism)(
            id => {
              implicit val executor = materializer.system.dispatcher
              session
                .map(_.collection(scopeName, collectionName))
                .flatMap(_.remove(id, removeOptions))
                .map(_ => CouchbaseDeleteSuccess(id))
                .recover(ex => CouchbaseDeleteFailure(id, ex.getCause))
            }
          )
      }
      .mapMaterializedValue(_ => NotUsed)
}
