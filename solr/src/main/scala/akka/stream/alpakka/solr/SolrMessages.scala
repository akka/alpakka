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
object IncomingMessage {
  // Apply methods to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(Upsert, None, None, None, None, Option(source), Map.empty, NotUsed)

  def apply[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(DeleteByIds, None, Option(id), None, None, None, Map.empty, NotUsed)

  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: Option[String],
               updates: Map[String, Map[String, Any]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(AtomicUpdate, Option(idField), Option(idValue), routingFieldValue, None, None, updates, NotUsed)

  // Apply methods to use with passThrough
  def apply[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(Upsert, None, None, None, None, Option(source), Map.empty, passThrough)

  def apply[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(DeleteByIds, None, Option(id), None, None, None, Map.empty, passThrough)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: Option[String],
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(AtomicUpdate, Option(idField), Option(idValue), routingFieldValue, None, None, updates, passThrough)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  def create[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(id)

  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(idField, idValue, Option(routingFieldValue), updates.asScala.toMap)

  def create[T](idField: String,
                idValue: String,
                updates: java.util.Map[String, Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(idField, idValue, None, updates.asScala.toMap)

  // Java-api - with passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

  def create[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(id, passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(idField, idValue, Option(routingFieldValue), updates.asScala.toMap, passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   updates: java.util.Map[String, Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(idField, idValue, None, updates.asScala.toMap, passThrough)

  def asScalaUpdates(jupdates: java.util.Map[String, java.util.Map[String, Object]]): Map[String, Map[String, Any]] =
    jupdates.asScala.map {
      case (k, v: java.util.Map[String, Object]) =>
        (k, v.asScala.toMap)
    }.toMap
}

object IncomingUpsertMessage {
  // Apply method to use when not using passThrough
  def apply[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingMessage(source)

  // Apply method to use when not using passThrough
  def apply[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(source, passThrough)

  // Java-api - without passThrough
  def create[T](source: T): IncomingMessage[T, NotUsed] =
    IncomingUpsertMessage[T](source)

  // Java-api - without passThrough
  def create[T, C](source: T, passThrough: C): IncomingMessage[T, C] =
    IncomingUpsertMessage[T, C](source, passThrough)
}

object IncomingDeleteMessageByIds {
  // Apply method to use when not using passThrough
  def apply[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(id)

  def apply[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(id, passThrough)

  // Java-api - without passThrough
  def create[T](id: String): IncomingMessage[T, NotUsed] =
    IncomingDeleteMessageByIds[T](id)

  def create[T, C](id: String, passThrough: C): IncomingMessage[T, C] =
    IncomingDeleteMessageByIds[T, C](id, passThrough)
}

object IncomingDeleteMessageByQuery {
  // Apply method to use when not using passThrough
  def apply[T](query: String): IncomingMessage[T, NotUsed] =
    IncomingMessage(DeleteByQuery, None, None, None, Some(query), None, Map.empty, NotUsed)

  def apply[T, C](query: String, passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(DeleteByQuery, None, None, None, Some(query), None, Map.empty, passThrough)

  // Java-api - without passThrough
  def create[T](query: String): IncomingMessage[T, NotUsed] =
    IncomingDeleteMessageByQuery[T](query)

  def create[T, C](query: String, passThrough: C): IncomingMessage[T, C] =
    IncomingDeleteMessageByQuery[T, C](query, passThrough)
}

object IncomingAtomicUpdateMessage {
  // Apply method to use when not using passThrough
  def apply[T](idField: String,
               idValue: String,
               routingFieldValue: Option[String],
               updates: Map[String, Map[String, Any]]): IncomingMessage[T, NotUsed] =
    IncomingMessage(idField, idValue, routingFieldValue, updates)

  def apply[T, C](idField: String,
                  idValue: String,
                  routingFieldValue: Option[String],
                  updates: Map[String, Map[String, Any]],
                  passThrough: C): IncomingMessage[T, C] =
    IncomingMessage(idField, idValue, routingFieldValue, updates, passThrough)

  // Java-api - without passThrough
  def create[T](idField: String,
                idValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingAtomicUpdateMessage[T](idField, idValue, None, IncomingMessage.asScalaUpdates(updates))

  def create[T](idField: String,
                idValue: String,
                routingFieldValue: String,
                updates: java.util.Map[String, java.util.Map[String, Object]]): IncomingMessage[T, NotUsed] =
    IncomingAtomicUpdateMessage[T](idField, idValue, Option(routingFieldValue), IncomingMessage.asScalaUpdates(updates))

  def create[T, C](idField: String,
                   idValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingAtomicUpdateMessage[T, C](idField, idValue, None, IncomingMessage.asScalaUpdates(updates), passThrough)

  def create[T, C](idField: String,
                   idValue: String,
                   routingFieldValue: String,
                   updates: java.util.Map[String, java.util.Map[String, Object]],
                   passThrough: C): IncomingMessage[T, C] =
    IncomingAtomicUpdateMessage[T, C](idField,
                                      idValue,
                                      Option(routingFieldValue),
                                      IncomingMessage.asScalaUpdates(updates),
                                      passThrough)
}

final case class IncomingMessage[T, C](operation: Operation,
                                       idField: Option[String],
                                       idFieldValue: Option[String],
                                       routingFieldValue: Option[String],
                                       query: Option[String],
                                       source: Option[T],
                                       updates: Map[String, Map[String, Any]],
                                       passThrough: C = NotUsed) {}

final case class IncomingMessageResult[T, C](idField: Option[String],
                                             idFieldValue: Option[String],
                                             routingFieldValue: Option[String],
                                             query: Option[String],
                                             source: Option[T],
                                             updates: Map[String, Map[String, Any]],
                                             passThrough: C,
                                             status: Int)



sealed trait Operation
final object Upsert extends Operation
final object DeleteByIds extends Operation
final object DeleteByQuery extends Operation
final object AtomicUpdate extends Operation

