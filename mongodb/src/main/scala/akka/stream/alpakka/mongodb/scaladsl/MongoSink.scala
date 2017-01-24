/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.mongodb.scaladsl

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.concurrent.Future

import org.mongodb.scala.Document
import org.mongodb.scala.MongoCollection

object MongoSink {
  def apply(parallelism: Int, collection: MongoCollection[Document]): Sink[Document, Future[Done]] =
    Flow[Document]
      .mapAsyncUnordered(parallelism)(doc => collection.insertOne(doc).toFuture())
      .toMat(Sink.ignore)(Keep.right)
}
