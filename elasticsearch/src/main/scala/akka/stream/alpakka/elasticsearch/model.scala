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

object IncomingIndexMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Index, None, Some(source))

  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Index, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(source)

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingIndexMessage(id, source)
}

object IncomingUpdateMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Update, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingUpdateMessage(id, source)
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Upsert, Some(id), Some(source))

  // Java-api - without passThrough
  def create[T](id: String, source: T): IncomingMessage[T, NotUsed] =
    IncomingUpsertMessage(id, source)
}

object IncomingDeleteMessage {
  // Apply method to use when not using passThrough
  def apply[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(Delete, Some(id), None)

  // Java-api - without passThrough
  def create[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingDeleteMessage(id)
}

case class IncomingMessage[T, C] private (
    operation: Operation,
    id: Option[String],
    source: Option[T],
    passThrough: C = NotUsed,
    version: Option[Long] = None,
    indexName: Option[String] = None,
    customMetadata: Map[String, String] = Map.empty
) {
  def withPassThrough[P](passThrough: P): IncomingMessage[T, P] =
    this.copy(passThrough = passThrough)

  def withVersion(version: Long): IncomingMessage[T, C] =
    this.copy(version = Option(version))

  def withIndexName(indexName: String): IncomingMessage[T, C] =
    this.copy(indexName = Option(indexName))

  /**
   * Scala API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(metadata: Map[String, String]): IncomingMessage[T, C] =
    this.copy(customMetadata = metadata)

  /**
   * Java API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(metadata: java.util.Map[String, String]): IncomingMessage[T, C] =
    this.copy(customMetadata = metadata.asScala.toMap)

}

case class IncomingMessageResult[T2, C2](message: IncomingMessage[T2, C2], error: Option[String]) {
  val success = error.isEmpty
}

trait MessageWriter[T] {
  def convert(message: T): String
}

final case class OutgoingMessage[T](id: String, source: T, version: Option[Long])

case class ScrollResponse[T](error: Option[String], result: Option[ScrollResult[T]])
case class ScrollResult[T](scrollId: String, messages: Seq[OutgoingMessage[T]])

trait MessageReader[T] {
  def convert(json: String): ScrollResponse[T]
}
