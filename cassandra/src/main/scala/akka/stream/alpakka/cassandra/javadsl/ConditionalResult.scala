/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import com.datastax.oss.driver.api.core.cql.AsyncResultSet

import scala.util.Either

trait ConditionalWriteResult[T] {
  def wasApplied: Boolean

  @throws(classOf[NoSuchElementException])
  def getContents: T

  @throws(classOf[NoSuchElementException])
  def getResultSet: AsyncResultSet
}

final class AppliedConditionalWriteResult[T](contents: T) extends ConditionalWriteResult[T] {
  def wasApplied: Boolean = true

  @throws(classOf[NoSuchElementException])
  override def getContents: T = contents

  @throws(classOf[NoSuchElementException])
  override def getResultSet: AsyncResultSet =
    throw new NoSuchElementException("No result set from Cassandra - conditional write was applied")
}

final class UnappliedConditionalWriteResult[T](resultSet: AsyncResultSet) extends ConditionalWriteResult[T] {
  def wasApplied: Boolean = false
  
  @throws(classOf[NoSuchElementException])
  override def getContents: T =
    throw new NoSuchElementException("Conditional write was not applied")

  @throws(classOf[NoSuchElementException])
  override def getResultSet: AsyncResultSet = resultSet
}

object ConditionalWriteResultBuilder {
  def applied[T](contents: T): AppliedConditionalWriteResult[T] = new AppliedConditionalWriteResult(contents)
  def unapplied[T](asyncResultSet: AsyncResultSet): UnappliedConditionalWriteResult[T] =
    new UnappliedConditionalWriteResult(asyncResultSet)

  def fromEither[T](either: Either[T, AsyncResultSet]): ConditionalWriteResult[T] =
    either.fold(applied, unapplied)
}
