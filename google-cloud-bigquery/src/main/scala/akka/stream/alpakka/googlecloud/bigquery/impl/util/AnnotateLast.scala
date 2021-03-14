/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.{Flow, Source}

@InternalApi
private[impl] object MaybeLast {
  def unapply[T](maybeLast: MaybeLast[T]): Some[T] = Some(maybeLast.value)
}

@InternalApi
private[impl] sealed abstract class MaybeLast[+T] {
  def value: T
  def isLast: Boolean
  def map[S](f: T => S): MaybeLast[S]
}

@InternalApi
private[impl] final case class NotLast[+T](value: T) extends MaybeLast[T] {
  override def isLast: Boolean = false
  override def map[S](f: T => S): NotLast[S] = NotLast(f(value))
}

@InternalApi
private[impl] final case class Last[+T](value: T) extends MaybeLast[T] {
  override def isLast: Boolean = true
  override def map[S](f: T => S): Last[S] = Last(f(value))
}

@InternalApi
private[impl] object AnnotateLast {

  def apply[T]: Flow[T, MaybeLast[T], NotUsed] =
    Flow[T]
      .map(Some(_))
      .concat(Source.single(None))
      .statefulMapConcat { () =>
        var previousElement: Option[T] = None
        e => {
          if (e.isDefined) {
            val emit = previousElement
            previousElement = e
            emit.map(NotLast(_)).toList
          } else {
            previousElement.map(Last(_)).toList
          }
        }
      }
}
