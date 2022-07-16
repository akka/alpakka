/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.typesense.IndexDocumentResult.IndexSuccess
import akka.stream.alpakka.typesense.{
  CollectionSchema,
  Field,
  FieldType,
  IndexDocument,
  IndexDocumentAction,
  IndexDocumentResult,
  IndexManyDocuments
}
import akka.stream.alpakka.typesense.integration.DocumentTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.scaladsl.Typesense
import spray.json.{JsonReader, JsonWriter}

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala
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

        describe("if already exist") {
          it("using flow") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
            val upsertDocument = upsertFromCreate(createDocument)
            val retrieve = retrieveDocumentFromIndexDocument(createDocument)

            //when
            val createResult = runWithFlow(createDocument, Typesense.indexDocumentFlow[Company](settings))
            val upsertResult = runWithFlow(upsertDocument, Typesense.indexDocumentFlow[Company](settings))
            val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }

          it("using sink") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
            val upsertDocument = upsertFromCreate(createDocument)
            val retrieve = retrieveDocumentFromIndexDocument(createDocument)

            //when
            val createResult = runWithSink(createDocument, Typesense.indexDocumentSink[Company](settings))
            val upsertResult = runWithSink(upsertDocument, Typesense.indexDocumentSink[Company](settings))
            val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }

          it("using direct request") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
            val upsertDocument = upsertFromCreate(createDocument)
            val retrieve = retrieveDocumentFromIndexDocument(createDocument)

            //when
            val createResult = Typesense.indexDocumentRequest(settings, createDocument).futureValue
            val upsertResult = Typesense.indexDocumentRequest(settings, upsertDocument).futureValue
            val retrieveResult = Typesense.retrieveDocumentRequest[Company](settings, retrieve).futureValue

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }

          it("using flow with Java API") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
            val upsertDocument = upsertFromCreate(createDocument)
            val retrieve = retrieveDocumentFromIndexDocument(createDocument)

            //when
            val createResult =
              runWithJavaFlow(createDocument,
                              JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]]))
            val upsertResult =
              runWithJavaFlow(upsertDocument,
                              JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]]))
            val retrieveResult =
              runWithJavaFlow(retrieve,
                              JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]]))

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }

          it("using sink with Java API") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
            val upsertDocument = upsertFromCreate(createDocument)
            val retrieve = retrieveDocumentFromIndexDocument(createDocument)

            //when
            val createResult =
              runWithJavaSink(createDocument,
                              JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
            val upsertResult =
              runWithJavaSink(upsertDocument,
                              JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
            val retrieveResult =
              runWithJavaFlow(retrieve,
                              JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]]))

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }

          it("using direct request with Java API") {
            //given
            val createDocument = randomIndexDocument(IndexDocumentAction.Create)
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
              .retrieveDocumentRequest[Company](settings, retrieve, system, implicitly[JsonReader[Company]])
              .asScala
              .futureValue

            //then
            createResult shouldBe Done
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }
        }

        describe("if doesn't exist") {
          it("using flow") {
            //given
            val upsertDocument = randomIndexDocument(IndexDocumentAction.Upsert)
            val retrieve = retrieveDocumentFromIndexDocument(upsertDocument)

            //when
            val upsertResult = runWithFlow(upsertDocument, Typesense.indexDocumentFlow[Company](settings))
            val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

            //then
            upsertResult shouldBe Done
            retrieveResult shouldBe upsertDocument.content
          }
        }
      }

      describe("with update action") {
        it("using flow") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val updateDocument =
            IndexDocument(indexDocument.collectionName,
                          UpdateCompany(id = indexDocument.content.id, budget = 7654),
                          IndexDocumentAction.Update)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)
          val expectedDocument = indexDocument.content.copy(budget = 7654)

          //when
          val createResult = runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          val updateResult = runWithFlow(updateDocument, Typesense.indexDocumentFlow[UpdateCompany](settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          updateResult shouldBe Done
          retrieveResult shouldBe expectedDocument
        }
      }

      describe("with emplace action") {
        describe("to create") {
          it("using flow") {
            //given
            val emplaceDocument = randomIndexDocument(IndexDocumentAction.Emplace)
            val retrieve = retrieveDocumentFromIndexDocument(emplaceDocument)

            //when
            val emplaceResult = runWithFlow(emplaceDocument, Typesense.indexDocumentFlow[Company](settings))
            val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

            //then
            emplaceResult shouldBe Done
            retrieveResult shouldBe emplaceDocument.content
          }
        }

        describe("to update") {
          it("using flow") {
            //given
            val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
            val emplaceDocument =
              IndexDocument(indexDocument.collectionName,
                            UpdateCompany(id = indexDocument.content.id, budget = 7654),
                            IndexDocumentAction.Emplace)
            val retrieve = retrieveDocumentFromIndexDocument(indexDocument)
            val expectedDocument = indexDocument.content.copy(budget = 7654)

            //when
            val createResult = runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            val emplaceResult = runWithFlow(emplaceDocument, Typesense.indexDocumentFlow[UpdateCompany](settings))
            val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

            //then
            createResult shouldBe Done
            emplaceResult shouldBe Done
            retrieveResult shouldBe expectedDocument
          }
        }
      }
    }

    describe("should not index single document") {
      describe("with update action") {
        describe("if document doesn't exist") {
          it("using flow") {
            val updateDocument =
              IndexDocument("companies",
                            UpdateCompany(id = UUID.randomUUID().toString, budget = 7654),
                            IndexDocumentAction.Update)

            tryUsingFlowAndExpectError(updateDocument,
                                       Typesense.indexDocumentFlow[UpdateCompany](settings),
                                       StatusCodes.NotFound)
          }
        }
      }
    }

    describe("should index many documents") {
      describe("with create action") {
        it("using flow") {
          //given
          val indexDocuments = IndexManyDocuments("companies", Seq(randomDocument(), randomDocument()))

          //when
          val indexResult = runWithFlow(indexDocuments, Typesense.indexManyDocumentsFlow[Company](settings))

          //then
          indexResult shouldBe Seq(IndexSuccess(), IndexSuccess())
          indexResult(0).isSuccess shouldBe true
          indexResult(1).isSuccess shouldBe true
        }

        it("using direct request") {
          //given
          val indexDocuments = IndexManyDocuments("companies", Seq(randomDocument(), randomDocument()))

          //when
          val indexResult = Typesense.indexManyDocumentsRequest(settings, indexDocuments).futureValue

          //then
          indexResult shouldBe Seq(IndexSuccess(), IndexSuccess())
        }

        it("using flow with Java API") {
          //given
          val indexDocuments = IndexManyDocuments("companies", Seq(randomDocument(), randomDocument()))

          //when
          val indexResult: util.List[IndexDocumentResult] =
            runWithJavaFlow(indexDocuments,
                            JavaTypesense.indexManyDocumentsFlow(settings, implicitly[JsonWriter[Company]]))

          //then
          indexResult.asScala shouldBe Seq(IndexSuccess(), IndexSuccess())
        }

        it("using direct request with Java API") {
          //given
          val indexDocuments = IndexManyDocuments("companies", Seq(randomDocument(), randomDocument()))

          //when
          val indexResult: util.List[IndexDocumentResult] =
            JavaTypesense
              .indexManyDocumentRequest(settings, indexDocuments, system, implicitly[JsonWriter[Company]])
              .asScala
              .futureValue

          //then
          indexResult.asScala shouldBe Seq(IndexSuccess(), IndexSuccess())
        }
      }
    }

    describe("should index only correct document") {
      describe("with create action") {
        describe("if another document already exist") {
          it("using flow") {
            //given
            val createSingleDocument: IndexDocument[Company] = randomIndexDocument()
            val createManyDocuments: IndexManyDocuments[Company] =
              IndexManyDocuments("companies", Seq(createSingleDocument.content, randomDocument()))

            //when
            val createSingleResult = Typesense.indexDocumentRequest(settings, createSingleDocument).futureValue
            val createManyResult = runWithFlow(createManyDocuments, Typesense.indexManyDocumentsFlow[Company](settings))

            //then
            createSingleResult shouldBe Done
            createManyResult(0).isSuccess shouldBe false
            createManyResult(1).isSuccess shouldBe true
          }
        }
      }
    }
  }
}
