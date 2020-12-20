/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.BigQuerySchemas._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BigQuerySchemasSpec extends AnyWordSpecLike with Matchers {

  case class A(integer: Int, long: Long, float: Float, double: Double, string: String, boolean: Boolean, record: B)
  case class B(nullable: Option[String], repeated: Seq[C])
  case class C(numeric: BigDecimal)

  val schema = TableSchema(
    List(
      TableFieldSchema("integer", IntegerType, Some(RequiredMode), None),
      TableFieldSchema("long", IntegerType, Some(RequiredMode), None),
      TableFieldSchema("float", FloatType, Some(RequiredMode), None),
      TableFieldSchema("double", FloatType, Some(RequiredMode), None),
      TableFieldSchema("string", StringType, Some(RequiredMode), None),
      TableFieldSchema("boolean", BooleanType, Some(RequiredMode), None),
      TableFieldSchema(
        "record",
        RecordType,
        Some(RequiredMode),
        Some(
          List(
            TableFieldSchema("nullable", StringType, Some(NullableMode), None),
            TableFieldSchema("repeated",
                             RecordType,
                             Some(RepeatedMode),
                             Some(List(TableFieldSchema("numeric", NumericType, Some(RequiredMode), None))))
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
