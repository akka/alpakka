/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.stream.alpakka.typesense.{Field, FieldResponse, FieldType}
import akka.stream.alpakka.typesense.integration.CollectionTypesenseIntegrationSpec

class CollectionTypesenseIntegrationSpec_V_0_23_0 extends CollectionTypesenseIntegrationSpec("0.23.0") {
  override protected def fieldResponseFromField(field: Field): FieldResponse = {
    val autoOptionalTypes: Set[FieldType] = Set(FieldType.StringAutoArray, FieldType.Auto)
    val optional = if (autoOptionalTypes.contains(field.`type`)) true else field.optional.getOrElse(false)
    val facet = field.facet.getOrElse(false)
    val index = field.index.getOrElse(true)

    FieldResponse(name = field.name,
                  `type` = field.`type`,
                  facet = facet,
                  optional = Some(optional),
                  index = Some(index))
  }

  describe(s"Only for Typesense $version") {
    describe("should create collection") {
      it("without specified default sorting field") {
        val fields = Seq(Field("company_nr", FieldType.Int32))
        createAndCheck(randomSchema(fields))
      }

      it("with specified empty default sorting field") {
        createAndCheck(randomSchema().withDefaultSortingField(""))
      }
    }
  }
}
