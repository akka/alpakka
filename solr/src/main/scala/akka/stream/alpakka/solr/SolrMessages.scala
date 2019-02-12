/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import akka.NotUsed
import akka.annotation.InternalApi

import scala.collection.JavaConverters._

object WriteMessage {
  def createUpsertMessage[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Upsert, None, None, None, None, Option(source), Map.empty, NotUsed)

  def createDeleteMessage[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage(DeleteByIds, None, Option(id), None, None, None, Map.empty, NotUsed)

  def createUpdateMessage[T](idField: String,
                             idValue: String,
                             routingFieldValue: Option[String],
                             updates: Map[String, Map[String, Any]]): WriteMessage[T, NotUsed] =
    WriteMessage(AtomicUpdate, Option(idField), Option(idValue), routingFieldValue, None, None, updates, NotUsed)

  /**
   * Java API
   */
  def createUpdateMessage[T](idField: String,
                             idValue: String,
                             routingFieldValue: String,
                             updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, Option(routingFieldValue), asScalaUpdates(updates))

  /**
   * Java API
   */
  def createUpdateMessage[T](idField: String,
                             idValue: String,
                             updates: java.util.Map[String, Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, None, updates.asScala.toMap)

  @InternalApi
  private[solr] def asScalaUpdates(
      jupdates: java.util.Map[String, java.util.Map[String, Object]]
  ): Map[String, Map[String, Any]] =
    jupdates.asScala.map {
      case (k, v: java.util.Map[String, Object]) =>
        (k, v.asScala.toMap)
    }.toMap
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage.createUpsertMessage(source)

  // Apply method to use when not using passThrough
  def apply[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    WriteMessage.createUpsertMessage(source).withPassThrough(passThrough)

  // Java-api - without passThrough
  def create[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage.createUpsertMessage(source)

  // Java-api - without passThrough
  def create[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    IncomingUpsertMessage[T, C](source, passThrough)
}

object IncomingDeleteMessageByIds {
  // Apply method to use when not using passThrough
  def apply[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage.createDeleteMessage(id)

  def apply[T, C](id: String, passThrough: C): WriteMessage[T, C] =
    WriteMessage.createDeleteMessage(id).withPassThrough(passThrough)

  // Java-api - without passThrough
  def create[T](id: String): WriteMessage[T, NotUsed] =
    IncomingDeleteMessageByIds[T](id)

  def create[T, C](id: String, passThrough: C): WriteMessage[T, C] =
    IncomingDeleteMessageByIds[T, C](id, passThrough)
}

object IncomingDeleteMessageByQuery {
  // Apply method to use when not using passThrough
  def apply[T](query: String): WriteMessage[T, NotUsed] =
    WriteMessage(DeleteByQuery, None, None, None, Some(query), None, Map.empty, NotUsed)

  def apply[T, C](query: String, passThrough: C): WriteMessage[T, C] =
    WriteMessage(DeleteByQuery, None, None, None, Some(query), None, Map.empty, passThrough)

  // Java-api - without passThrough
  def create[T](query: String): WriteMessage[T, NotUsed] =
    IncomingDeleteMessageByQuery[T](query)

  def create[T, C](query: String, passThrough: C): WriteMessage[T, C] =
    IncomingDeleteMessageByQuery[T, C](query, passThrough)
}

object IncomingAtomicUpdateMessage {
  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: Option[String],
               updates: Map[String, Map[String, Any]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, routingFieldValue, updates)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: Option[String],
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): WriteMessage[T, C] =
    WriteMessage.createUpdateMessage(idField, idValue, routingFieldValue, updates).withPassThrough(passThrough)

  /**
   * Java API
   */
  def create[T](idField: String,
                idValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, None, WriteMessage.asScalaUpdates(updates))

  /**
   * Java API
   */
  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage.createUpdateMessage(idField, idValue, Option(routingFieldValue), WriteMessage.asScalaUpdates(updates))

  /**
   * Java API
   */
  def create[T, C](idField: String,
                   idValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    WriteMessage
      .createUpdateMessage(idField, idValue, None, WriteMessage.asScalaUpdates(updates))
      .withPassThrough(passThrough)

  /**
   * Java API
   */
  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    WriteMessage
      .createUpdateMessage(idField, idValue, Option(routingFieldValue), WriteMessage.asScalaUpdates(updates))
      .withPassThrough(passThrough)
}

final case class WriteMessage[T, C](operation: Operation,
                                    idField: Option[String],
                                    idFieldValue: Option[String],
                                    routingFieldValue: Option[String],
                                    query: Option[String],
                                    source: Option[T],
                                    updates: Map[String, Map[String, Any]],
                                    passThrough: C = NotUsed) {

  def withPassThrough[PT2](value: PT2): WriteMessage[T, PT2] =
    new WriteMessage(operation, idField, idFieldValue, routingFieldValue, query, source, updates, value)

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
