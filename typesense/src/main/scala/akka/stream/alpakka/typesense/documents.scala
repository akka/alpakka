/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

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

final class IndexManyDocuments[T] @InternalApi private[typesense] (val collectionName: String,
                                                                   val documents: Seq[T],
                                                                   val action: IndexDocumentAction) {
  def withCollectionName(collectionName: String): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents, action)

  def withAction(action: IndexDocumentAction): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents, action)

  def getDocuments(): java.util.List[T] = documents.asJava

  override def equals(other: Any): Boolean = other match {
    case that: IndexManyDocuments[_] =>
      collectionName == that.collectionName &&
      documents == that.documents &&
      action == that.action
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(collectionName, documents, action)

  override def toString = s"IndexManyDocuments(collectionName=$collectionName, documents=$documents, action=$action)"
}

object IndexManyDocuments {
  def apply[T](collectionName: String, documents: Seq[T]): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents, IndexDocumentAction.Create)
  def create[T](collectionName: String, documents: java.util.List[T]): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents.asScala.toSeq, IndexDocumentAction.Create)
  def apply[T](collectionName: String, documents: Seq[T], action: IndexDocumentAction): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents, action)
  def create[T](collectionName: String,
                documents: java.util.List[T],
                action: IndexDocumentAction): IndexManyDocuments[T] =
    new IndexManyDocuments(collectionName, documents.asScala.toSeq, action)
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

sealed trait IndexDocumentResult {
  def isSuccess: Boolean
}

object IndexDocumentResult {
  final class IndexSuccess @InternalApi private[typesense] () extends IndexDocumentResult {
    override def equals(other: Any): Boolean = other match {
      case _: IndexSuccess => true
      case _ => false
    }

    override def hashCode(): Int = 1

    override def toString = s"IndexSuccess"

    override def isSuccess: Boolean = true
  }

  object IndexSuccess {
    def apply(): IndexSuccess = new IndexSuccess()
    def create(): IndexSuccess = new IndexSuccess()
  }

  final class IndexFailure @InternalApi private[typesense] (val error: String, val document: String)
      extends IndexDocumentResult {
    override def equals(other: Any): Boolean = other match {
      case that: IndexFailure =>
        error == that.error &&
        document == that.document
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(error, document)

    override def toString = s"IndexFailure(error=$error, document=$document)"

    override def isSuccess: Boolean = false
  }

  object IndexFailure {
    def apply(error: String, document: String): IndexFailure = new IndexFailure(error, document)
    def create(error: String, document: String): IndexFailure = new IndexFailure(error, document)
  }
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
