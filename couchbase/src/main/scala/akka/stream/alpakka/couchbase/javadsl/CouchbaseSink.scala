/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import java.util
import java.util.concurrent.CompletionStage
import akka.stream.alpakka.couchbase._
import akka.stream.javadsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document

object CouchbaseSink {

  def upsertSingle[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                     couchbaseBucket: Bucket): Sink[T, CompletionStage[Done]] =
    Flow
      .fromGraph(CouchbaseFlow.upsertSingle[T](couchbaseWriteSettings, couchbaseBucket))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def deleteSingle(couchbaseWriteSettings: CouchbaseWriteSettings,
                   couchbaseBucket: Bucket): Sink[String, CompletionStage[Done]] =
    Flow
      .fromGraph(CouchbaseFlow.deleteOne(couchbaseWriteSettings, couchbaseBucket))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def upsertBulk[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                   couchbaseBucket: Bucket): Sink[util.List[T], CompletionStage[Done]] =
    Flow
      .fromGraph(CouchbaseFlow.upsertBulk[T](couchbaseWriteSettings, couchbaseBucket))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

  def deleteBulk(couchbaseWriteSettings: CouchbaseWriteSettings,
                 couchbaseBucket: Bucket): Sink[util.List[String], CompletionStage[Done]] =
    Flow
      .fromGraph(CouchbaseFlow.deleteBulk(couchbaseWriteSettings, couchbaseBucket))
      .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])

}
