/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.stream.alpakka.typesense.FilterDeleteDocumentsQuery

object FilterDeleteDocumentsQueryDsl {
  def stringQuery(query: String): FilterDeleteDocumentsQuery = new FilterDeleteDocumentsQuery {
    override def asTextQuery: String = query
  }

  def inStringSet(field: String, values: Seq[String]): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:[${values.distinct.mkString(",")}]"
    }

  def inIntSet(field: String, values: Seq[Int]): FilterDeleteDocumentsQuery =
    inStringSet(field, values.distinct.map(_.toString))

  def biggerThanInt(field: String, value: Int): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:>$value"
    }

  def biggerThanFloat(field: String, value: Double): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:>$value"
    }

  def biggerThanOrEqualInt(field: String, value: Int): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:>=$value"
    }

  def biggerThanOrEqualFloat(field: String, value: Double): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:>=$value"
    }

  def lowerThanInt(field: String, value: Int): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:<$value"
    }

  def lowerThanFloat(field: String, value: Double): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:<$value"
    }

  def lowerThanOrEqualInt(field: String, value: Int): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:<=$value"
    }

  def lowerThanOrEqualFloat(field: String, value: Double): FilterDeleteDocumentsQuery =
    new FilterDeleteDocumentsQuery {
      override def asTextQuery: String = s"$field:<=$value"
    }
}
