/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  BooleanType,
  FloatType,
  IntegerType,
  NumericType,
  StringType,
  TableFieldSchema
}

/**
 * Provides the Big Query schemas for the most important Scala types.
 */
trait BasicSchemas {

  private final class PrimitiveSchemaWriter[T](`type`: String) extends SchemaWriter[T] {
    override def write(name: String, mode: FieldMode): TableFieldSchema = {
      TableFieldSchema(name, `type`, Some(mode.toString), None)
    }
  }

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
