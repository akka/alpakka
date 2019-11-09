/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.annotation.InternalApi

import scala.collection.JavaConverters._

final class SqsBatchException[T <: AnyRef] @InternalApi private[sqs] (val errors: List[SqsResultErrorEntry[T]])
    extends Exception(s"SQS batch operation failed with ${errors.size} errors") {

  /** Java API */
  def getErrors: java.util.List[SqsResultErrorEntry[T]] = errors.asJava

  override def toString: String = s"SqsBatchException(errors=$errors)"

  override def equals(other: Any): Boolean = other match {
    case that: SqsBatchException[T] => java.util.Objects.equals(this.errors, that.errors)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(errors)
}
