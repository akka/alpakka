package akka.stream.alpakka.typesense.integration.v0_11_2

import akka.stream.alpakka.typesense.integration.CollectionTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.{Field, FieldResponse}

class CollectionTypesenseIntegrationSpec_V_0_11_2 extends CollectionTypesenseIntegrationSpec("0.11.2") {
  override protected def fieldResponseFromField(field: Field): FieldResponse =
    FieldResponse(name = field.name,
                  `type` = field.`type`,
                  facet = field.facet.getOrElse(false),
                  optional = field.optional,
                  index = field.index)
}
