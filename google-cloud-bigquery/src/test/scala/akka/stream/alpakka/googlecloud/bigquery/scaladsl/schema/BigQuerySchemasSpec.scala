/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model._
import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaType._
import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaMode._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.BigQuerySchemas._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BigQuerySchemasSpec extends AnyWordSpecLike with Matchers {

  case class A(integer: Int, long: Long, float: Float, double: Double, string: String, boolean: Boolean, record: B)
  case class B(nullable: Option[String], repeated: Seq[C])
  case class C(numeric: BigDecimal)

  val schema = TableSchema(
    List(
      TableFieldSchema("integer", Integer, Some(Required), None),
      TableFieldSchema("long", Integer, Some(Required), None),
      TableFieldSchema("float", Float, Some(Required), None),
      TableFieldSchema("double", Float, Some(Required), None),
      TableFieldSchema("string", String, Some(Required), None),
      TableFieldSchema("boolean", Boolean, Some(Required), None),
      TableFieldSchema(
        "record",
        Record,
        Some(Required),
        Some(
          List(
            TableFieldSchema("nullable", String, Some(Nullable), None),
            TableFieldSchema("repeated",
                             Record,
                             Some(Repeated),
                             Some(List(TableFieldSchema("numeric", Numeric, Some(Required), None))))
          )
        )
      )
    )
  )

  "BigQuerySchemas" should {

    "correctly generate schema" in {
      implicit val cSchemaWriter = bigQuerySchema1(C)
      implicit val bSchemaWriter = bigQuerySchema2(B)
      val generatedSchema = bigQuerySchema7(A).write
      generatedSchema shouldEqual schema
    }

    "throw exception when nesting options" in {
      case class Invalid(invalid: Option[Option[String]])
      assertThrows[IllegalArgumentException](bigQuerySchema1(Invalid).write)
    }

    "throw exception when nesting options inside seqs" in {
      case class Invalid(invalid: Seq[Option[String]])
      assertThrows[IllegalArgumentException](bigQuerySchema1(Invalid).write)
    }
  }
}
