/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi
import akka.http.scaladsl.model.StatusCode

sealed trait TypesenseResult[+T] {
  def map[S](f: T => S): TypesenseResult[S]
  def get(): Option[T]
}

final class SuccessTypesenseResult[T] @InternalApi private[typesense] (val value: T) extends TypesenseResult[T] {
  override def map[S](f: T => S): TypesenseResult[S] = new SuccessTypesenseResult(f(value))

  override def get(): Option[T] = Some(value)

  override def equals(other: Any): Boolean = other match {
    case that: SuccessTypesenseResult[_] =>
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(value)

  override def toString = s"SuccessTypesenseResult(value=$value)"
}

final class FailureTypesenseResult @InternalApi private[typesense] (val statusCode: StatusCode, val reason: String)
    extends TypesenseResult[Nothing] { self =>

  override def map[S](f: Nothing => S): TypesenseResult[S] = self

  override def get(): Option[Nothing] = None

  override def equals(other: Any): Boolean = other match {
    case that: FailureTypesenseResult =>
      statusCode == that.statusCode &&
      reason == that.reason
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(statusCode, reason)

  override def toString = s"FailureTypesenseResult(statusCode=$statusCode, reason=$reason)"
}
