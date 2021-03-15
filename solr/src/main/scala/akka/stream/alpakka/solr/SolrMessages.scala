/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import akka.NotUsed
import akka.annotation.InternalApi

import scala.collection.JavaConverters._

object WriteMessage {
  def createUpsertMessage[T](source: T): WriteMessage[T, NotUsed] =
    new WriteMessage(Upsert, source = Option(source))

  def createDeleteMessage[T](id: String): WriteMessage[T, NotUsed] =
    new WriteMessage(DeleteByIds, idFieldValue = Option(id))

  def createDeleteByQueryMessage[T](query: String): WriteMessage[T, NotUsed] =
    new WriteMessage(DeleteByQuery, query = Option(query))

  def createUpdateMessage[T](idField: String,
                             idValue: String,
                             updates: Map[String, Map[String, Any]]): WriteMessage[T, NotUsed] =
    new WriteMessage(AtomicUpdate,
                     idField = Option(idField),
                     idFieldValue = Option(idValue),
                     routingFieldValue = None,
                     updates = updates)

  /**
   * Java API
   */
  def createUpdateMessage[T](idField: String,
                             idValue: String,
                             updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, asScalaUpdates(updates))

  @InternalApi
  private[solr] def asScalaUpdates(
      jupdates: java.util.Map[String, java.util.Map[String, Object]]
  ): Map[String, Map[String, Any]] =
    jupdates.asScala.map {
      case (k, v: java.util.Map[String, Object]) =>
        (k, v.asScala.toMap)
    }.toMap

  def createPassThrough[C](passThrough: C): WriteMessage[NotUsed, C] =
    new WriteMessage(PassThrough).withPassThrough(passThrough)
}

final class WriteMessage[T, C] private (
    val operation: Operation,
    val idField: Option[String] = None,
    val idFieldValue: Option[String] = None,
    val routingFieldValue: Option[String] = None,
    val query: Option[String] = None,
    val source: Option[T] = None,
    val updates: Map[String, Map[String, Any]] = Map.empty,
    val passThrough: C = NotUsed
) {

  def withIdFieldValue(idField: String, idFieldValue: String): WriteMessage[T, C] =
    copy(idField = Option(idField), idFieldValue = Some(idFieldValue))

  def withIdFieldValue(value: String): WriteMessage[T, C] = copy(idFieldValue = Option(value))
  def withRoutingFieldValue(value: String): WriteMessage[T, C] = copy(routingFieldValue = Option(value))
  def withQuery(value: String): WriteMessage[T, C] = copy(query = Option(value))

  def withSource[T2](value: T2): WriteMessage[T2, C] =
    new WriteMessage(operation, idField, idFieldValue, routingFieldValue, query, Option(value), updates, passThrough)

  def withUpdates(value: Map[java.lang.String, Map[String, Any]]): WriteMessage[T, C] =
    copy(updates = value)

  /** Java API */
  def withUpdates(value: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, C] =
    copy(updates = WriteMessage.asScalaUpdates(value))

  def withPassThrough[PT2](value: PT2): WriteMessage[T, PT2] =
    new WriteMessage(operation, idField, idFieldValue, routingFieldValue, query, source, updates, value)

  private def copy(
      operation: Operation = operation,
      idField: Option[String] = idField,
      idFieldValue: Option[String] = idFieldValue,
      routingFieldValue: Option[String] = routingFieldValue,
      query: Option[String] = query,
      source: Option[T] = source,
      updates: Map[String, Map[String, Any]] = updates,
      passThrough: C = passThrough
  ): WriteMessage[T, C] = new WriteMessage[T, C](
    operation = operation,
    idField = idField,
    idFieldValue = idFieldValue,
    routingFieldValue = routingFieldValue,
    query = query,
    source = source,
    updates = updates,
    passThrough = passThrough
  )

  override def toString =
    "WriteMessage(" +
    s"operation=$operation," +
    s"idField=$idField," +
    s"idFieldValue=$idFieldValue," +
    s"routingFieldValue=$routingFieldValue," +
    s"query=$query," +
    s"source=$source," +
    s"updates=$updates," +
    s"passThrough=$passThrough" +
    ")"
}

final case class WriteResult[T, C](idField: Option[String],
                                   idFieldValue: Option[String],
                                   routingFieldValue: Option[String],
                                   query: Option[String],
                                   source: Option[T],
                                   updates: Map[String, Map[String, Any]],
                                   passThrough: C,
                                   status: Int)

sealed trait Operation
object Upsert extends Operation
object DeleteByIds extends Operation
object DeleteByQuery extends Operation
object AtomicUpdate extends Operation
object PassThrough extends Operation
