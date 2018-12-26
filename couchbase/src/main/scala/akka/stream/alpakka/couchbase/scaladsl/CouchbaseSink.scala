/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.Done
import akka.stream.alpakka.couchbase._
import akka.stream.scaladsl.{Keep, Sink}
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.Bucket
import scala.concurrent.Future
import scala.collection.immutable.Seq

object CouchbaseSink {

  def upsertSingle[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                     couchbaseBucket: Bucket): Sink[T, Future[Done]] =
    CouchbaseFlow.upsertSingle(couchbaseWriteSettings, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def deleteOne(couchbaseWriteSettings: CouchbaseWriteSettings, couchbaseBucket: Bucket): Sink[String, Future[Done]] =
    CouchbaseFlow.deleteOne(couchbaseWriteSettings, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def deleteBulk(couchbaseWriteSettings: CouchbaseWriteSettings,
                 couchbaseBucket: Bucket): Sink[Seq[String], Future[Done]] =
    CouchbaseFlow.deleteBulk(couchbaseWriteSettings, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def upsertBulk[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                   couchbaseBucket: Bucket): Sink[Seq[T], Future[Done]] =
    CouchbaseFlow.upsertBulk(couchbaseWriteSettings, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

}
