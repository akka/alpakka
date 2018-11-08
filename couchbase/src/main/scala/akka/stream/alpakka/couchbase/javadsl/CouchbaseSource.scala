/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlQuery, Statement}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object CouchbaseSource {

  def fromN1qlQuery(query: N1qlQuery, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] =
    akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource.fromN1qlQuery(query, couchbaseBucket).asJava

  def fromStatement(statement: Statement, couchbaseBucket: Bucket): Source[AsyncN1qlQueryRow, NotUsed] =
    Source.fromGraph(akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource.fromStatement(statement, couchbaseBucket))

  def fromSingleId[T <: Document[_]](id: String, couchbaseBucket: Bucket, target: Class[T]): Source[T, NotUsed] =
    Source.fromGraph(
      akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource.fromSingleId[T](id, couchbaseBucket, target)
    )

  def fromIdBulk[T <: Document[_]](ids: java.util.List[String],
                                   couchbaseBucket: Bucket,
                                   target: Class[T]): Source[T, NotUsed] = {
    val idSeq: Seq[String] = ids.asScala.to[Seq]
    Source.fromGraph(
      akka.stream.alpakka.couchbase.scaladsl.CouchbaseSource.fromBulkIds[T](idSeq, couchbaseBucket, target)
    )

  }

}
