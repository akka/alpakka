/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.stream.alpakka.typesense.{CollectionSchema, Field, FieldType}
import akka.stream.alpakka.typesense.integration.DocumentTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.scaladsl.Typesense

class DocumentTypesenseIntegrationSpec_V_0_23_0 extends DocumentTypesenseIntegrationSpec("0.23.0") {
  override protected def createCompaniesCollection(): Unit = {
    val schema = CollectionSchema(
      "companies",
      Seq(Field("id", FieldType.String), Field("name", FieldType.String), Field("budget", FieldType.Int32))
    )
    Typesense.createCollectionRequest(settings, schema).futureValue
  }
}
