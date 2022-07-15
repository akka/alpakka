/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi

final class IndexDocument[T] @InternalApi private[typesense] (val collectionName: String,
                                                              val content: T,
                                                              val action: IndexDocumentAction) {
  def withCollectionName(collectionName: String): IndexDocument[T] =
    new IndexDocument(collectionName, content, action)

  def withAction(action: IndexDocumentAction): IndexDocument[T] =
    new IndexDocument(collectionName, content, action)

  def withContent(content: T): IndexDocument[T] =
    new IndexDocument(collectionName, content, action)

  override def equals(other: Any): Boolean = other match {
    case that: IndexDocument[_] =>
      collectionName == that.collectionName &&
      content == that.content &&
      action == that.action
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(collectionName, content, action)

  override def toString = s"IndexDocument(collectionName=$collectionName, content=$content, action=$action)"
}

object IndexDocument {
  def apply[T](collectionName: String, content: T): IndexDocument[T] =
    new IndexDocument(collectionName, content, IndexDocumentAction.Create)
  def create[T](collectionName: String, content: T): IndexDocument[T] =
    new IndexDocument(collectionName, content, IndexDocumentAction.Create)
  def apply[T](collectionName: String, content: T, action: IndexDocumentAction): IndexDocument[T] =
    new IndexDocument(collectionName, content, action)
  def create[T](collectionName: String, content: T, action: IndexDocumentAction): IndexDocument[T] =
    new IndexDocument(collectionName, content, action)
}

sealed abstract class IndexDocumentAction

object IndexDocumentAction {
  sealed abstract class Create extends IndexDocumentAction
  case object Create extends Create

  /**
   * Java API
   */
  def create: Create = Create

  sealed abstract class Upsert extends IndexDocumentAction
  case object Upsert extends Upsert

  /**
   * Java API
   */
  def upsert: Upsert = Upsert

  sealed abstract class Update extends IndexDocumentAction
  case object Update extends Update

  /**
   * Java API
   */
  def update: Update = Update

  sealed abstract class Emplace extends IndexDocumentAction
  case object Emplace extends Emplace

  /**
   * Java API
   */
  def emplace: Emplace = Emplace
}

final class RetrieveDocument @InternalApi private[typesense] (val collectionName: String, val documentId: String) {

  override def equals(other: Any): Boolean = other match {
    case that: RetrieveDocument =>
      collectionName == that.collectionName &&
      documentId == that.documentId
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(collectionName, documentId)

  override def toString = s"RetrieveDocument(collectionName=$collectionName, documentId=$documentId)"
}

object RetrieveDocument {
  def apply(collectionName: String, documentId: String): RetrieveDocument =
    new RetrieveDocument(collectionName, documentId)
  def create(collectionName: String, documentId: String): RetrieveDocument =
    new RetrieveDocument(collectionName, documentId)
}
