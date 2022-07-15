/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.Done
import akka.stream.alpakka.typesense.{CollectionSchema, Field, FieldType, IndexDocument, IndexDocumentAction}
import akka.stream.alpakka.typesense.integration.DocumentTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.scaladsl.Typesense
import spray.json.{JsonReader, JsonWriter}

import scala.jdk.FutureConverters.CompletionStageOps

class DocumentTypesenseIntegrationSpec_V_0_23_0 extends DocumentTypesenseIntegrationSpec("0.23.0") {
  import DocumentTypesenseIntegrationSpec._

  override protected def createCompaniesCollection(): Unit = {
    val schema = CollectionSchema(
      "companies",
      Seq(Field("id", FieldType.String), Field("name", FieldType.String), Field("budget", FieldType.Int32))
    )
    Typesense.createCollectionRequest(settings, schema).futureValue
  }

  describe(s"Only for Typesense $version") {
    describe("should index and retrieve single document") {
      describe("with upsert action") {
        def upsertFromCreate(createDocument: IndexDocument[Company]): IndexDocument[Company] =
          createDocument
            .withAction(IndexDocumentAction.Upsert)
            .withContent(createDocument.content.copy(name = "New Name"))

        it("using flow") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult = runWithFlow(createDocument, Typesense.indexDocumentFlow(settings))
          val upsertResult = runWithFlow(upsertDocument, Typesense.indexDocumentFlow(settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow(settings))

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }

        it("using sink") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult = runWithSink(createDocument, Typesense.indexDocumentSink(settings))
          val upsertResult = runWithSink(upsertDocument, Typesense.indexDocumentSink(settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow(settings))

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }

        it("using direct request") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult = Typesense.indexDocumentRequest(settings, createDocument).futureValue
          val upsertResult = Typesense.indexDocumentRequest(settings, upsertDocument).futureValue
          val retrieveResult = Typesense.retrieveDocumentRequest(settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }

        it("using flow with Java API") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult =
            runWithJavaFlow(createDocument, JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]))
          val upsertResult =
            runWithJavaFlow(upsertDocument, JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]))
          val retrieveResult =
            runWithJavaFlow(retrieve, JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]]))

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }

        it("using sink with Java API") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult =
            runWithJavaSink(createDocument, JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
          val upsertResult =
            runWithJavaSink(upsertDocument, JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
          val retrieveResult =
            runWithJavaFlow(retrieve, JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]]))

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }

        it("using direct request with Java API") {
          //given
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val retrieve = retrieveDocumentFromIndexDocument(createDocument)

          //when
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, createDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val upsertResult = JavaTypesense
            .indexDocumentRequest(settings, upsertDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val retrieveResult = JavaTypesense
            .retrieveDocumentRequest(settings, retrieve, system, implicitly[JsonReader[Company]])
            .asScala
            .futureValue

          //then
          createResult shouldBe Done
          upsertResult shouldBe Done
          retrieveResult shouldBe upsertDocument.content
        }
      }
    }
  }
}
