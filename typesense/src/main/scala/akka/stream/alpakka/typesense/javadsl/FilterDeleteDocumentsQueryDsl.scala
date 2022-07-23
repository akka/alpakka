/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.javadsl

import akka.stream.alpakka.typesense.FilterDeleteDocumentsQuery

import scala.jdk.CollectionConverters.ListHasAsScala

object FilterDeleteDocumentsQueryDsl {
  private val ScalaFilterDeleteDocumentsQuery = akka.stream.alpakka.typesense.scaladsl.FilterDeleteDocumentsQueryDsl

  def stringQuery(query: String): FilterDeleteDocumentsQuery = ScalaFilterDeleteDocumentsQuery.stringQuery(query)

  def inStringSet(field: String, values: java.util.List[String]): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.inStringSet(field, values.asScala.toSeq)

  def inIntSet(field: String, values: java.util.List[java.lang.Integer]): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.inIntSet(field, values.asScala.toSeq.map((i: Integer) => i.toInt))

  def biggerThanInt(field: String, value: java.lang.Integer): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.biggerThanInt(field, value.toInt)

  def biggerThanFloat(field: String, value: java.lang.Double): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.biggerThanFloat(field, value.toDouble)

  def biggerThanOrEqualInt(field: String, value: java.lang.Integer): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.biggerThanOrEqualInt(field, value.toInt)

  def biggerThanOrEqualFloat(field: String, value: java.lang.Double): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.biggerThanOrEqualFloat(field, value.toDouble)

  def lowerThanInt(field: String, value: java.lang.Integer): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.lowerThanInt(field, value.toInt)

  def lowerThanFloat(field: String, value: java.lang.Double): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.lowerThanFloat(field, value.toDouble)

  def lowerThanOrEqualInt(field: String, value: java.lang.Integer): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.lowerThanOrEqualInt(field, value.toInt)

  def lowerThanOrEqualFloat(field: String, value: java.lang.Double): FilterDeleteDocumentsQuery =
    ScalaFilterDeleteDocumentsQuery.lowerThanOrEqualFloat(field, value.toDouble)
}
