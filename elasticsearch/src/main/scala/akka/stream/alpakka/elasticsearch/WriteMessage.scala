/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import scala.collection.JavaConverters._

// TODO move to impl?
sealed trait Operation
object Index extends Operation
object Update extends Operation
object Upsert extends Operation
object Delete extends Operation

object WriteIndexMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Index, None, Some(source))

  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Index, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](source: T): WriteMessage[T, NotUsed] =
    WriteIndexMessage(source)

  // Java-api - without passThrough
  def create[T](id: String, source: T): WriteMessage[T, NotUsed] =
    WriteIndexMessage(id, source)
}

object IncomingUpdateMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Update, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](id: String, source: T): WriteMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source)
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Upsert, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](id: String, source: T): WriteMessage[T, NotUsed] =
    IncomingUpsertMessage(id, source)
}

object IncomingDeleteMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage(Delete, Some(id), None)

  // Java-api - without passThrough
  def create[T](id: String): WriteMessage[T, NotUsed] =
    IncomingDeleteMessage(id)
}

case class WriteMessage[T, C] private (
    operation: Operation,
    id: Option[String],
    source: Option[T],
    passThrough: C = NotUsed,
    version: Option[Long] = None,
    indexName: Option[String] = None,
    customMetadata: Map[String, String] = Map.empty
) {
  def withPassThrough[P](passThrough: P): WriteMessage[T, P] =
    this.copy(passThrough = passThrough)

  def withVersion(version: Long): WriteMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String): WriteMessage[T, C] =
    this.copy(indexName = Option(indexName))

  /**
   * Scala API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(metadata: Map[String, String]): WriteMessage[T, C] =
    this.copy(customMetadata = metadata)

  /**
   * Java API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(metadata: java.util.Map[String, String]): WriteMessage[T, C] =
    this.copy(customMetadata = metadata.asScala.toMap)

}

case class WriteResult[T2, C2](message: WriteMessage[T2, C2], error: Option[String]) {
  val success = error.isEmpty
}

trait MessageWriter[T] {
  def convert(message: T): String
}
