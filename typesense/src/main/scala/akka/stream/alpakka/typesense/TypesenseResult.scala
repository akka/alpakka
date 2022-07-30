/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi
import akka.http.scaladsl.model.StatusCode

sealed trait TypesenseResult[T]

final class SuccessTypesenseResult[T] @InternalApi private[typesense] (val value: T) extends TypesenseResult[T] {

  override def equals(other: Any): Boolean = other match {
    case that: SuccessTypesenseResult[_] =>
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(value)

  override def toString = s"SuccessTypesenseResult(value=$value)"
}

final class FailureTypesenseResult[T] @InternalApi private[typesense] (val statusCode: StatusCode, val reason: String)
    extends TypesenseResult[T] {

  override def equals(other: Any): Boolean = other match {
    case that: FailureTypesenseResult[_] =>
      statusCode == that.statusCode &&
      reason == that.reason
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(statusCode, reason)

  override def toString = s"FailureTypesenseResult(statusCode=$statusCode, reason=$reason)"
}
