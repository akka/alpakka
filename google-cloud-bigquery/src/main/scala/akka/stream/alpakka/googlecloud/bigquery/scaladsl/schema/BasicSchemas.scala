/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  BooleanType,
  FloatType,
  IntegerType,
  NumericType,
  StringType
}

/**
 * Provides the BigQuery schemas for the most important Scala types.
 */
trait BasicSchemas {
  implicit val intSchemaWriter: SchemaWriter[Int] = new PrimitiveSchemaWriter(IntegerType)
  implicit val longSchemaWriter: SchemaWriter[Long] = new PrimitiveSchemaWriter(IntegerType)
  implicit val floatSchemaWriter: SchemaWriter[Float] = new PrimitiveSchemaWriter[Float](FloatType)
  implicit val doubleSchemaWriter: SchemaWriter[Double] = new PrimitiveSchemaWriter[Double](FloatType)
  implicit val byteSchemaWriter: SchemaWriter[Byte] = new PrimitiveSchemaWriter(IntegerType)
  implicit val shortSchemaWriter: SchemaWriter[Short] = new PrimitiveSchemaWriter(IntegerType)
  implicit val bigDecimalSchemaWriter: SchemaWriter[BigDecimal] = new PrimitiveSchemaWriter(NumericType)
  implicit val booleanSchemaWriter: SchemaWriter[Boolean] = new PrimitiveSchemaWriter(BooleanType)
  implicit val charSchemaWriter: SchemaWriter[Char] = new PrimitiveSchemaWriter(StringType)
  implicit val stringSchemaWriter: SchemaWriter[String] = new PrimitiveSchemaWriter(StringType)
}
