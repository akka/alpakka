/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.mongodb.{DocumentReplace, DocumentUpdate}
import akka.stream.alpakka.mongodb.scaladsl.MongoFlow.{
  DefaultDeleteOptions,
  DefaultInsertManyOptions,
  DefaultInsertOneOptions,
  DefaultReplaceOptions,
  DefaultUpdateOptions
}
import akka.stream.javadsl.{Keep, Sink}
import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateOptions}
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.conversions.Bson

object MongoSink {

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will insert documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   */
  def insertOne[T](collection: MongoCollection[T]): Sink[T, CompletionStage[Done]] =
    insertOne(collection, DefaultInsertOneOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will insert documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertOne[T](collection: MongoCollection[T], options: InsertOneOptions): Sink[T, CompletionStage[Done]] =
    MongoFlow.insertOne(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will insert batches of documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   */
  def insertMany[T](collection: MongoCollection[T]): Sink[java.util.List[T], CompletionStage[Done]] =
    insertMany(collection, DefaultInsertManyOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will insert batches of documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertMany[T](collection: MongoCollection[T],
                    options: InsertManyOptions): Sink[java.util.List[T], CompletionStage[Done]] =
    MongoFlow.insertMany(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will update documents as defined by a [[akka.stream.alpakka.mongodb.DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   */
  def updateOne[T](collection: MongoCollection[T]): Sink[DocumentUpdate, CompletionStage[Done]] =
    updateOne(collection, DefaultUpdateOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will update documents as defined by a [[akka.stream.alpakka.mongodb.DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateOne[T](collection: MongoCollection[T],
                   options: UpdateOptions): Sink[DocumentUpdate, CompletionStage[Done]] =
    MongoFlow.updateOne(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   */
  def updateMany[T](collection: MongoCollection[T]): Sink[DocumentUpdate, CompletionStage[Done]] =
    updateMany(collection, DefaultUpdateOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateMany[T](collection: MongoCollection[T],
                    options: UpdateOptions): Sink[DocumentUpdate, CompletionStage[Done]] =
    MongoFlow.updateMany(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will delete individual documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   */
  def deleteOne[T](collection: MongoCollection[T]): Sink[Bson, CompletionStage[Done]] =
    deleteOne(collection, DefaultDeleteOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will delete individual documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteOne[T](collection: MongoCollection[T], options: DeleteOptions): Sink[Bson, CompletionStage[Done]] =
    MongoFlow.deleteOne(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will delete many documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   */
  def deleteMany[T](collection: MongoCollection[T]): Sink[Bson, CompletionStage[Done]] =
    deleteMany(collection, DefaultDeleteOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will delete many documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteMany[T](collection: MongoCollection[T], options: DeleteOptions): Sink[Bson, CompletionStage[Done]] =
    MongoFlow.deleteMany(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will replace document as defined by a [[akka.stream.alpakka.mongodb.DocumentReplace]].
   *
   * @param collection the mongo db collection to update.
   */
  def replaceOne[T](collection: MongoCollection[T]): Sink[DocumentReplace[T], CompletionStage[Done]] =
    replaceOne(collection, DefaultReplaceOptions)

  /**
   * A [[akka.stream.javadsl.Sink Sink]] that will replace document as defined by a [[akka.stream.alpakka.mongodb.DocumentReplace]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def replaceOne[T](
      collection: MongoCollection[T],
      options: ReplaceOptions
  ): Sink[DocumentReplace[T], CompletionStage[Done]] =
    MongoFlow.replaceOne(collection, options).toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

}
