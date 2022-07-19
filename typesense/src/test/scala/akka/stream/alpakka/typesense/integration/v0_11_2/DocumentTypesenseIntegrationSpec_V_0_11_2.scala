/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_11_2

import akka.stream.alpakka.typesense.{CollectionSchema, Field, FieldType}
import akka.stream.alpakka.typesense.integration.DocumentTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.scaladsl.Typesense

class DocumentTypesenseIntegrationSpec_V_0_11_2 extends DocumentTypesenseIntegrationSpec("0.11.2") {
  override protected def createCollection(name: String): Unit = {
    val schema = CollectionSchema(
      name,
      Seq(Field("id", FieldType.String), Field("name", FieldType.String), Field("budget", FieldType.Int32))
    ).withDefaultSortingField("budget")
    Typesense.createCollectionRequest(settings, schema).futureValue
  }
}
