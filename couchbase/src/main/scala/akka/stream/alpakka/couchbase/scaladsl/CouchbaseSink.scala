/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.scaladsl

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.{Bucket, PersistTo, ReplicateTo}

import scala.concurrent.{ExecutionContext, Future}

object CouchbaseSink {

  def upsertSingle[T <: Document[_]](parallelism: Int, couchbaseBucket: Bucket)(
      implicit executionContext: ExecutionContext
  ): Sink[T, Future[Done]] = CouchbaseFlow.upsertSingle(parallelism, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def upsertSingle[T <: Document[_]](
      parallelism: Int,
      couchbaseBucket: Bucket,
      persistTo: PersistTo,
      replicateTo: ReplicateTo
  )(implicit executionContext: ExecutionContext): Sink[T, Future[Done]] =
    CouchbaseFlow.upsertSingle(parallelism, couchbaseBucket, persistTo, replicateTo).toMat(Sink.ignore)(Keep.right)

  def deleteOne(parallism: Int,
                couchbaseBucket: Bucket)(implicit executionContext: ExecutionContext): Sink[String, Future[Done]] =
    CouchbaseFlow.deleteOne(parallism, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def deleteOne(parallism: Int, couchbaseBucket: Bucket, persistTo: PersistTo, replicateTo: ReplicateTo)(
      implicit executionContext: ExecutionContext
  ): Sink[String, Future[Done]] =
    CouchbaseFlow.deleteOne(parallism, couchbaseBucket, persistTo, replicateTo).toMat(Sink.ignore)(Keep.right)

  def deleteBulk(parallism: Int, couchbaseBucket: Bucket, persistTo: PersistTo, replicateTo: ReplicateTo)(
      implicit executionContext: ExecutionContext
  ): Sink[Seq[String], Future[Done]] =
    CouchbaseFlow.deleteBulk(parallism, couchbaseBucket, persistTo, replicateTo).toMat(Sink.ignore)(Keep.right)

  def deleteBulk(parallism: Int, couchbaseBucket: Bucket)(
      implicit executionContext: ExecutionContext
  ): Sink[Seq[String], Future[Done]] =
    CouchbaseFlow.deleteBulk(parallism, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def upsertBulk[T <: Document[_]](parallelism: Int, couchbaseBucket: Bucket)(
      implicit executionContext: ExecutionContext
  ): Sink[Seq[T], Future[Done]] =
    CouchbaseFlow.upsertBulk(parallelism, couchbaseBucket).toMat(Sink.ignore)(Keep.right)

  def upsertBulk[T <: Document[_]](
      parallelism: Int,
      couchbaseBucket: Bucket,
      persistTo: PersistTo,
      replicateTo: ReplicateTo
  )(implicit executionContext: ExecutionContext): Sink[Seq[T], Future[Done]] =
    CouchbaseFlow.upsertBulk(parallelism, couchbaseBucket, persistTo, replicateTo).toMat(Sink.ignore)(Keep.right)

}
