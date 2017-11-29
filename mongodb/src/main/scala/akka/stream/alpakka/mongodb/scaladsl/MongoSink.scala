/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future
import org.mongodb.scala.{Document, MongoCollection, SingleObservable}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions

object MongoSink {
  def insertOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Document, Future[Done]] =
    run[Document](parallelism)(doc => collection.insertOne(doc))

  def insertMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Seq[Document], Future[Done]] =
    run[Seq[Document]](parallelism)(docs => collection.insertMany(docs))

  final case class DocumentUpdate(filter: Bson, update: Bson)

  def updateOne(parallelism: Int,
                collection: MongoCollection[Document],
                maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    maybeUpdateOptions match {
      case None =>
        run[DocumentUpdate](parallelism)(
          documentUpdate => collection.updateOne(documentUpdate.filter, documentUpdate.update)
        )
      case Some(options) =>
        run[DocumentUpdate](parallelism)(
          documentUpdate => collection.updateOne(documentUpdate.filter, documentUpdate.update, options)
        )
    }

  def updateMany(parallelism: Int,
                 collection: MongoCollection[Document],
                 maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    maybeUpdateOptions match {
      case None =>
        run[DocumentUpdate](parallelism)(
          documentUpdate => collection.updateMany(documentUpdate.filter, documentUpdate.update)
        )
      case Some(options) =>
        run[DocumentUpdate](parallelism)(
          documentUpdate => collection.updateMany(documentUpdate.filter, documentUpdate.update, options)
        )
    }

  def deleteOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    run[Bson](parallelism)(collection.deleteOne)

  def deleteMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    run[Bson](parallelism)(collection.deleteMany)

  private def run[A](parallelism: Int)(fn: A => SingleObservable[_]): Sink[A, Future[Done]] =
    Flow[A]
      .mapAsyncUnordered(parallelism)(fn(_).toFuture())
      .toMat(Sink.ignore)(Keep.right)
}
