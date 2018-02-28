/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.Flow
import akka.NotUsed
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.result.{DeleteResult, UpdateResult}
import org.mongodb.scala.MongoCollection

import scala.concurrent.ExecutionContext

object MongoFlow {

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will insert documents into a collection.
   * @param parallelism number of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertOne[T](parallelism: Int, collection: MongoCollection[T])(
      implicit executionContext: ExecutionContext
  ): Flow[T, T, NotUsed] =
    Flow[T]
      .mapAsync(parallelism)(doc => collection.insertOne(doc).toFuture().map(_ => doc))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will insert batches documents into a collection.
   * @param parallelism number of batches of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertMany[T](parallelism: Int, collection: MongoCollection[T])(
      implicit executionContext: ExecutionContext
  ): Flow[Seq[T], Seq[T], NotUsed] =
    Flow[Seq[T]].mapAsync(parallelism)(docs => collection.insertMany(docs).toFuture().map(_ => docs))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateOne[T](
      parallelism: Int,
      collection: MongoCollection[T],
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
   * A [[akka.stream.scaladsl.Flow Flow]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateMany[T](
      parallelism: Int,
      collection: MongoCollection[T],
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
   * A [[akka.stream.scaladsl.Flow Flow]] that will delete individual documents as defined by a [[org.mongodb.scala.bson.conversions.Bson Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteOne[T](parallelism: Int, collection: MongoCollection[T])(
      implicit executionContext: ExecutionContext
  ): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].mapAsync(parallelism)(bson => collection.deleteOne(bson).toFuture().map(_ -> bson))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will delete many documents as defined by a [[org.mongodb.scala.bson.conversions.Bson Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteMany[T](parallelism: Int, collection: MongoCollection[T])(
      implicit executionContext: ExecutionContext
  ): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].mapAsync(parallelism)(bson => collection.deleteMany(bson).toFuture().map(_ -> bson))
}
