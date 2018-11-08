/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import java.util
import akka.NotUsed
import akka.stream.javadsl.Flow
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import akka.stream.alpakka.couchbase.{BulkOperationResult, CouchbaseWriteSettings, SingleOperationResult}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
object CouchbaseFlow {

  def deleteOne(couchbaseWriteSettings: CouchbaseWriteSettings,
                couchbaseBucket: Bucket): Flow[String, SingleOperationResult[String], NotUsed] =
    Flow.fromGraph(
      akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
        .deleteOne(couchbaseWriteSettings, couchbaseBucket)
    )

  def upsertSingle[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                     couchbaseBucket: Bucket): Flow[T, SingleOperationResult[T], NotUsed] =
    Flow.fromGraph(
      akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
        .upsertSingle[T](couchbaseWriteSettings, couchbaseBucket)
    )

  def deleteBulk(couchbaseWriteSettings: CouchbaseWriteSettings,
                 couchbaseBucket: Bucket): Flow[util.List[String], BulkOperationResult[String], NotUsed] =
    Flow.fromGraph(
      akka.stream.scaladsl
        .Flow[util.List[String]]
        .map(_.asScala.to[Seq])
        .via(
          akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
            .deleteBulk(couchbaseWriteSettings, couchbaseBucket)
        )
    )

  def fromSingleId[T <: Document[_]](couchbaseBucket: Bucket, target: Class[T]): Flow[String, T, NotUsed] =
    Flow.fromGraph(akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow.fromSingleId(couchbaseBucket, target))

  def fromBulkId[T <: Document[_]](couchbaseBucket: Bucket,
                                   target: Class[T]): Flow[util.List[String], util.List[T], NotUsed] =
    Flow.fromGraph(
      akka.stream.scaladsl
        .Flow[util.List[String]]
        .map(_.asScala.to[Seq])
        .via(akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow.fromBulkIds(couchbaseBucket, target))
        .map(f => f.asJava)
    )

  def upsertBulk[T <: Document[_]](couchbaseWriteSettings: CouchbaseWriteSettings,
                                   couchbaseBucket: Bucket): Flow[util.List[T], BulkOperationResult[T], NotUsed] =
    Flow.fromGraph(
      akka.stream.scaladsl
        .Flow[util.List[T]]
        .map(_.asScala.to[Seq])
        .via(
          akka.stream.alpakka.couchbase.scaladsl.CouchbaseFlow
            .upsertBulk[T](couchbaseWriteSettings, couchbaseBucket)
        )
    )

}
