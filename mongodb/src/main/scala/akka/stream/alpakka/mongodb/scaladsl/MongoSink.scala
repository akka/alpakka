/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.result.{DeleteResult, UpdateResult}
import org.mongodb.scala.{Completed, Document, MongoCollection}

import scala.concurrent.Future

object MongoSink {

  /**
   * A [[Sink]] that will insert documents into a collection.
   * @param parallelism number of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Document, Future[Done]] =
    insertOneFlow(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will insert documents into a collection.
   * @param parallelism number of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertOneFlow(parallelism: Int, collection: MongoCollection[Document]): Flow[Document, Completed, NotUsed] =
    Flow[Document]
      .mapAsyncUnordered(parallelism)(doc => collection.insertOne(doc).toFuture())

  /**
   * A [[Sink]] that will insert batches of documents into a collection.
   * @param parallelism number of batches of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Seq[Document], Future[Done]] =
    insertManyFlow(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will insert batches documents into a collection.
   * @param parallelism number of batches of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertManyFlow(parallelism: Int, collection: MongoCollection[Document]) =
    Flow[Seq[Document]].mapAsyncUnordered(parallelism)(docs => collection.insertMany(docs).toFuture())

  /**
   * A [[Sink]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateOne(parallelism: Int,
                collection: MongoCollection[Document],
                maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    updateOneFlow(parallelism, collection, maybeUpdateOptions).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateOneFlow(parallelism: Int,
                    collection: MongoCollection[Document],
                    maybeUpdateOptions: Option[UpdateOptions] = None): Flow[DocumentUpdate, UpdateResult, NotUsed] =
    maybeUpdateOptions match {
      case None =>
        Flow[DocumentUpdate].mapAsyncUnordered(parallelism)(
          documentUpdate => collection.updateOne(documentUpdate.filter, documentUpdate.update).toFuture()
        )
      case Some(options) =>
        Flow[DocumentUpdate].mapAsyncUnordered(parallelism)(
          documentUpdate => collection.updateOne(documentUpdate.filter, documentUpdate.update, options).toFuture()
        )
    }

  /**
   * A [[Sink]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateMany(parallelism: Int,
                 collection: MongoCollection[Document],
                 maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    updateManyFlow(parallelism, collection, maybeUpdateOptions).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateManyFlow(parallelism: Int,
                     collection: MongoCollection[Document],
                     maybeUpdateOptions: Option[UpdateOptions] = None): Flow[DocumentUpdate, UpdateResult, NotUsed] =
    maybeUpdateOptions match {
      case None =>
        Flow[DocumentUpdate].mapAsyncUnordered(parallelism)(
          documentUpdate => collection.updateMany(documentUpdate.filter, documentUpdate.update).toFuture()
        )
      case Some(options) =>
        Flow[DocumentUpdate].mapAsyncUnordered(parallelism)(
          documentUpdate => collection.updateMany(documentUpdate.filter, documentUpdate.update, options).toFuture()
        )
    }

  /**
   * A [[Sink]] that will delete individual documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    deleteOneFlow(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will delete individual documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteOneFlow(parallelism: Int, collection: MongoCollection[Document]): Flow[Bson, DeleteResult, NotUsed] =
    Flow[Bson].mapAsyncUnordered(parallelism)(collection.deleteOne(_).toFuture())

  /**
   * A [[Sink]] that will delete many documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    deleteManyFlow(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[Flow]] that will delete many documents as defined by a [[Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteManyFlow(parallelism: Int, collection: MongoCollection[Document]): Flow[Bson, DeleteResult, NotUsed] =
    Flow[Bson].mapAsyncUnordered(parallelism)(collection.deleteMany(_).toFuture())

}
