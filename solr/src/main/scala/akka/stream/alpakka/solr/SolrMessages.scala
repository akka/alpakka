/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import akka.NotUsed
import scala.collection.JavaConverters._

@deprecated(
  "you should use a specific incoming message case class: IncomingUpsertMessage/IncomingDeleteMessageByIds/IncomingDeleteMessageByQuery/IncomingAtomicUpdateMessage",
  "0.20"
)
object WriteMessage {
  // Apply methods to use when not using passThrough
  def apply[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage(Upsert, None, None, None, None, Option(source), Map.empty, NotUsed)

  def apply[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage(DeleteByIds, None, Option(id), None, None, None, Map.empty, NotUsed)

  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: Option[String],
               updates: Map[String, Map[String, Any]]): WriteMessage[T, NotUsed] =
    WriteMessage(AtomicUpdate, Option(idField), Option(idValue), routingFieldValue, None, None, updates, NotUsed)

  // Apply methods to use with passThrough
  def apply[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    WriteMessage(Upsert, None, None, None, None, Option(source), Map.empty, passThrough)

  def apply[T, C](id: String, passThrough: C): WriteMessage[T, C] =
    WriteMessage(DeleteByIds, None, Option(id), None, None, None, Map.empty, passThrough)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: Option[String],
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): WriteMessage[T, C] =
    WriteMessage(AtomicUpdate, Option(idField), Option(idValue), routingFieldValue, None, None, updates, passThrough)

  // Java-api - without passThrough
  def create[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage(source)

  def create[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage(id)

  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage(idField, idValue, Option(routingFieldValue), updates.asScala.toMap)

  def create[T](idField: String,
                idValue: String,
                updates: java.util.Map[String, Map[String, Object]]): WriteMessage[T, NotUsed] =
    WriteMessage(idField, idValue, None, updates.asScala.toMap)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    WriteMessage(source, passThrough)

  def create[T, C](id: String, passThrough: C): WriteMessage[T, C] =
    WriteMessage(id, passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    WriteMessage(idField, idValue, Option(routingFieldValue), updates.asScala.toMap, passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   updates: java.util.Map[String, Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    WriteMessage(idField, idValue, None, updates.asScala.toMap, passThrough)

  def asScalaUpdates(jupdates: java.util.Map[String, java.util.Map[String, Object]]): Map[String, Map[String, Any]] =
    jupdates.asScala.map {
      case (k, v: java.util.Map[String, Object]) =>
        (k, v.asScala.toMap)
    }.toMap
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): WriteMessage[T, NotUsed] =
    WriteMessage(source)

  // Apply method to use when not using passThrough
  def apply[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    WriteMessage(source, passThrough)

  // Java-api - without passThrough
  def create[T](source: T): WriteMessage[T, NotUsed] =
    IncomingUpsertMessage[T](source)

  // Java-api - without passThrough
  def create[T, C](source: T, passThrough: C): WriteMessage[T, C] =
    IncomingUpsertMessage[T, C](source, passThrough)
}

object IncomingDeleteMessageByIds {
  // Apply method to use when not using passThrough
  def apply[T](id: String): WriteMessage[T, NotUsed] =
    WriteMessage(id)

  def apply[T, C](id: String, passThrough: C): WriteMessage[T, C] =
    WriteMessage(id, passThrough)

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
  // Apply method to use when not using passThrough
  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: Option[String],
               updates: Map[String, Map[String, Any]]): WriteMessage[T, NotUsed] =
    WriteMessage(idField, idValue, routingFieldValue, updates)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: Option[String],
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): WriteMessage[T, C] =
    WriteMessage(idField, idValue, routingFieldValue, updates, passThrough)

  // Java-api - without passThrough
  def create[T](idField: String,
                idValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    IncomingAtomicUpdateMessage[T](idField, idValue, None, WriteMessage.asScalaUpdates(updates))

  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): WriteMessage[T, NotUsed] =
    IncomingAtomicUpdateMessage[T](idField, idValue, Option(routingFieldValue), WriteMessage.asScalaUpdates(updates))

  def create[T, C](idField: String,
                   idValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    IncomingAtomicUpdateMessage[T, C](idField, idValue, None, WriteMessage.asScalaUpdates(updates), passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): WriteMessage[T, C] =
    IncomingAtomicUpdateMessage[T, C](idField,
                                      idValue,
                                      Option(routingFieldValue),
                                      WriteMessage.asScalaUpdates(updates),
                                      passThrough)
}

final case class WriteMessage[T, C](operation: Operation,
                                    idField: Option[String],
                                    idFieldValue: Option[String],
                                    routingFieldValue: Option[String],
                                    query: Option[String],
                                    source: Option[T],
                                    updates: Map[String, Map[String, Any]],
                                    passThrough: C = NotUsed) {}

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
