/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mongodb.scaladsl

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future
import org.mongodb.scala.Document
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.UpdateOptions

object MongoSink {
  def insertOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Document, Future[Done]] =
    Flow[Document]
      .mapAsyncUnordered(parallelism)(doc => collection.insertOne(doc).toFuture())
      .toMat(Sink.ignore)(Keep.right)

  def insertMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Seq[Document], Future[Done]] =
    Flow[Seq[Document]]
      .mapAsyncUnordered(parallelism)(docs => collection.insertMany(docs).toFuture())
      .toMat(Sink.ignore)(Keep.right)

  def updateOne(parallelism: Int,
                collection: MongoCollection[Document],
                maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    Flow[DocumentUpdate]
      .mapAsyncUnordered(parallelism)(
        documentUpdate => {
          maybeUpdateOptions match {
            case None =>
              collection.updateOne(documentUpdate.filter, documentUpdate.update)
            case Some(options) =>
              collection.updateOne(documentUpdate.filter, documentUpdate.update, options)
          }
        }.toFuture()
      )
      .toMat(Sink.ignore)(Keep.right)

  def updateMany(parallelism: Int,
                 collection: MongoCollection[Document],
                 maybeUpdateOptions: Option[UpdateOptions] = None): Sink[DocumentUpdate, Future[Done]] =
    Flow[DocumentUpdate]
      .mapAsyncUnordered(parallelism)(
        documentUpdate => {
          maybeUpdateOptions match {
            case None =>
              collection.updateMany(documentUpdate.filter, documentUpdate.update)
            case Some(options) =>
              collection.updateMany(documentUpdate.filter, documentUpdate.update, options)
          }
        }.toFuture()
      )
      .toMat(Sink.ignore)(Keep.right)

  def deleteOne(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    Flow[Bson]
      .mapAsyncUnordered(parallelism)(filter => collection.deleteOne(filter).toFuture())
      .toMat(Sink.ignore)(Keep.right)

  def deleteMany(parallelism: Int, collection: MongoCollection[Document]): Sink[Bson, Future[Done]] =
    Flow[Bson]
      .mapAsyncUnordered(parallelism)(filter => collection.deleteMany(filter).toFuture())
      .toMat(Sink.ignore)(Keep.right)

  final case class DocumentUpdate(filter: Bson, update: Bson)
}
