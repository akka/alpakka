/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.{Flow, Source}
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.mongodb.DocumentUpdate
import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, UpdateOptions}
import com.mongodb.client.result.{DeleteResult, UpdateResult}
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.conversions.Bson

import scala.collection.JavaConverters._

object MongoFlow {

  /** Internal Api */
  @InternalApi private[mongodb] val DefaultInsertOneOptions = new InsertOneOptions()

  /** Internal Api */
  @InternalApi private[mongodb] val DefaultInsertManyOptions = new InsertManyOptions()

  /** Internal Api */
  @InternalApi private[mongodb] val DefaultUpdateOptions = new UpdateOptions()

  /** Internal Api */
  @InternalApi private[mongodb] val DefaultDeleteOptions = new DeleteOptions()

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will insert documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertOne[T](collection: MongoCollection[T],
                   options: InsertOneOptions = DefaultInsertOneOptions): Flow[T, T, NotUsed] =
    Flow[T]
      .flatMapConcat(doc => Source.fromPublisher(collection.insertOne(doc, options)).map(_ => doc))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will insert batches documents into a collection.
   *
   * @param collection mongo db collection to insert to.
   * @param options options to apply to the operation
   */
  def insertMany[T](collection: MongoCollection[T],
                    options: InsertManyOptions = DefaultInsertManyOptions): Flow[Seq[T], Seq[T], NotUsed] =
    Flow[Seq[T]].flatMapConcat(docs => Source.fromPublisher(collection.insertMany(docs.asJava, options)).map(_ => docs))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will update documents as defined by a [[DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateOne[T](
      collection: MongoCollection[T],
      options: UpdateOptions = DefaultUpdateOptions
  ): Flow[DocumentUpdate, (UpdateResult, DocumentUpdate), NotUsed] =
    Flow[DocumentUpdate].flatMapConcat(
      documentUpdate =>
        Source
          .fromPublisher(collection.updateOne(documentUpdate.filter, documentUpdate.update, options))
          .map(_ -> documentUpdate)
    )

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will update many documents as defined by a [[DocumentUpdate]].
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def updateMany[T](
      collection: MongoCollection[T],
      options: UpdateOptions = DefaultUpdateOptions
  ): Flow[DocumentUpdate, (UpdateResult, DocumentUpdate), NotUsed] =
    Flow[DocumentUpdate].flatMapConcat(
      documentUpdate =>
        Source
          .fromPublisher(collection.updateMany(documentUpdate.filter, documentUpdate.update, options))
          .map(_ -> documentUpdate)
    )

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will delete individual documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteOne[T](collection: MongoCollection[T],
                   options: DeleteOptions = DefaultDeleteOptions): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].flatMapConcat(bson => Source.fromPublisher(collection.deleteOne(bson, options)).map(_ -> bson))

  /**
   * A [[akka.stream.scaladsl.Flow Flow]] that will delete many documents as defined by a [[org.bson.conversions.Bson Bson]] filter query.
   *
   * @param collection the mongo db collection to update.
   * @param options options to apply to the operation
   */
  def deleteMany[T](collection: MongoCollection[T],
                    options: DeleteOptions = DefaultDeleteOptions): Flow[Bson, (DeleteResult, Bson), NotUsed] =
    Flow[Bson].flatMapConcat(bson => Source.fromPublisher(collection.deleteMany(bson, options)).map(_ -> bson))
}
