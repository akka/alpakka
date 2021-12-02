/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.{Keep, Sink}
import akka.Done
import akka.stream.alpakka.mongodb.{DocumentReplace, DocumentUpdate}
import akka.stream.alpakka.mongodb.scaladsl.MongoFlow.{
  DefaultDeleteOptions,
  DefaultInsertManyOptions,
  DefaultInsertOneOptions,
  DefaultReplaceOptions,
  DefaultUpdateOptions
}
import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateOptions}
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.conversions.Bson

import scala.concurrent.Future

object MongoSink {

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will insert documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertOne[T](collection: MongoCollection[T],
                   options: InsertOneOptions = DefaultInsertOneOptions): Sink[T, Future[Done]] =
    MongoFlow.insertOne(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will insert batches of documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertMany[T](collection: MongoCollection[T],
                    options: InsertManyOptions = DefaultInsertManyOptions): Sink[Seq[T], Future[Done]] =
    MongoFlow.insertMany(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will update documents as defined by a [[akka.stream.alpakka.mongodb.DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateOne[T](collection: MongoCollection[T],
                   options: UpdateOptions = DefaultUpdateOptions): Sink[DocumentUpdate, Future[Done]] =
    MongoFlow.updateOne(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateMany[T](
      collection: MongoCollection[T],
      options: UpdateOptions = DefaultUpdateOptions
  ): Sink[DocumentUpdate, Future[Done]] =
    MongoFlow.updateMany(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will delete individual documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteOne[T](collection: MongoCollection[T],
                   options: DeleteOptions = DefaultDeleteOptions): Sink[Bson, Future[Done]] =
    MongoFlow.deleteOne(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will delete many documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteMany[T](collection: MongoCollection[T],
                    options: DeleteOptions = DefaultDeleteOptions): Sink[Bson, Future[Done]] =
    MongoFlow.deleteMany(collection, options).toMat(Sink.ignore)(Keep.right)

  /**
   * A [[akka.stream.scaladsl.Sink Sink]] that will replace document as defined by a [[akka.stream.alpakka.mongodb.DocumentReplace]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def replaceOne[T](
      collection: MongoCollection[T],
      options: ReplaceOptions = DefaultReplaceOptions
  ): Sink[DocumentReplace[T], Future[Done]] =
    MongoFlow.replaceOne(collection, options).toMat(Sink.ignore)(Keep.right)
}
