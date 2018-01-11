/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.Flow
import akka.NotUsed
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.result.{DeleteResult, UpdateResult}
import org.mongodb.scala.{Document, MongoCollection}

import scala.concurrent.ExecutionContext

object MongoFlow {

  /**
   * A [[Flow]] that will insert documents into a collection.
   * @param parallelism number of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertOne(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Flow[Document, Document, NotUsed] =
    Flow[Document]
      .mapAsync(parallelism)(doc => collection.insertOne(doc).toFuture().map(_ => doc))

  /**
   * A [[Flow]] that will insert batches documents into a collection.
   * @param parallelism number of batches of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertMany(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Flow[Seq[Document], Seq[Document], NotUsed] =
    Flow[Seq[Document]].mapAsync(parallelism)(docs => collection.insertMany(docs).toFuture().map(_ => docs))

  /**
   * A [[Flow]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateOne(
      parallelism: Int,
      collection: MongoCollection[Document],
      maybeUpdateOptions: Option[UpdateOptions] = None
  )(implicit executionContext: ExecutionContext): Flow[DocumentUpdate, (UpdateResult, DocumentUpdate), NotUsed] =
    maybeUpdateOptions match {
      case None =>
        Flow[DocumentUpdate].mapAsync(parallelism)(
          documentUpdate =>
            collection.updateOne(documentUpdate.filter, documentUpdate.update).toFuture().map(_ -> documentUpdate)
        )
      case Some(options) =>
        Flow[DocumentUpdate].mapAsync(parallelism)(
          documentUpdate =>
            collection
              .updateOne(documentUpdate.filter, documentUpdate.update, options)
              .toFuture()
              .map(_ -> documentUpdate)
        )
    }

  /**
   * A [[Flow]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateMany(
      parallelism: Int,
      collection: MongoCollection[Document],
      maybeUpdateOptions: Option[UpdateOptions] = None
  )(implicit executionContext: ExecutionContext): Flow[DocumentUpdate, (UpdateResult, DocumentUpdate), NotUsed] =
    maybeUpdateOptions match {
      case None =>
        Flow[DocumentUpdate].mapAsync(parallelism)(
          documentUpdate =>
            collection.updateMany(documentUpdate.filter, documentUpdate.update).toFuture().map(_ -> documentUpdate)
        )
      case Some(options) =>
        Flow[DocumentUpdate].mapAsync(parallelism)(
          documentUpdate =>
            collection
              .updateMany(documentUpdate.filter, documentUpdate.update, options)
              .toFuture()
              .map(_ -> documentUpdate)
        )
    }

  /**
   * A [[Flow]] that will delete individual documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteOne(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].mapAsync(parallelism)(bson => collection.deleteOne(bson).toFuture().map(_ -> bson))

  /**
   * A [[Flow]] that will delete many documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteMany(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].mapAsync(parallelism)(bson => collection.deleteMany(bson).toFuture().map(_ -> bson))
}
