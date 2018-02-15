/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.{Keep, Sink}
import akka.Done
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.{Document, MongoCollection}

import scala.concurrent.{ExecutionContext, Future}

object MongoSink {

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will insert documents into a collection.
   * @param parallelism number of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertOne(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Sink[Document, Future[Done]] =
    MongoFlow.insertOne(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will insert batches of documents into a collection.
   * @param parallelism number of batches of documents to insert in parallel.
   * @param collection mongo db collection to insert to.
   */
  def insertMany(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Sink[Seq[Document], Future[Done]] =
    MongoFlow.insertMany(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateOne(
      parallelism: Int,
      collection: MongoCollection[Document],
      maybeUpdateOptions: Option[UpdateOptions] = None
  )(implicit executionContext: ExecutionContext): Sink[DocumentUpdate, Future[Done]] =
    MongoFlow.updateOne(parallelism, collection, maybeUpdateOptions).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param parallelism the number of documents to update in parallel.
   * @param collection the mongo db collection to update.
   * @param maybeUpdateOptions optional additional [[UpdateOptions]]
   */
  def updateMany(
      parallelism: Int,
      collection: MongoCollection[Document],
      maybeUpdateOptions: Option[UpdateOptions] = None
  )(implicit executionContext: ExecutionContext): Sink[DocumentUpdate, Future[Done]] =
    MongoFlow.updateMany(parallelism, collection, maybeUpdateOptions).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will delete individual documents as defined by a [[org.mongodb.scala.bson.conversions.Bson Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteOne(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Sink[Bson, Future[Done]] =
    MongoFlow.deleteOne(parallelism, collection).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will delete many documents as defined by a [[org.mongodb.scala.bson.conversions.Bson Bson]] filter query.
   *
   * @param parallelism the number of documents to delete in parallel.
   * @param collection the mongo db collection to update.
   */
  def deleteMany(parallelism: Int, collection: MongoCollection[Document])(
      implicit executionContext: ExecutionContext
  ): Sink[Bson, Future[Done]] =
    MongoFlow.deleteMany(parallelism, collection).toMat(Sink.ignore)(Keep.right)

}
