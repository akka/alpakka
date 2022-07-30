/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration.v0_23_0

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.typesense.IndexDocumentResult.IndexSuccess
import akka.stream.alpakka.typesense.integration.DocumentTypesenseIntegrationSpec
import akka.stream.alpakka.typesense.scaladsl.{FilterDeleteDocumentsQueryDsl, Typesense}
import akka.stream.alpakka.typesense._
import org.scalatest.Assertion
import spray.json.{JsonReader, JsonWriter}

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class DocumentTypesenseIntegrationSpec_V_0_23_0 extends DocumentTypesenseIntegrationSpec("0.23.0") {
  import DocumentTypesenseIntegrationSpec._

  override protected def createCollection(name: String): Unit = {
    val schema = CollectionSchema(
      name,
      Seq(Field("id", FieldType.String),
          Field("name", FieldType.String),
          Field("budget", FieldType.Int32),
          Field("evaluation", FieldType.Float))
    )
    runWithFlow(schema, Typesense.createCollectionFlow(settings))
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
            val createSingleResult = runWithFlow(createSingleDocument, Typesense.indexDocumentFlow[Company](settings))
            val createManyResult = runWithFlow(createManyDocuments, Typesense.indexManyDocumentsFlow[Company](settings))

            //then
            createSingleResult shouldBe Done
            createManyResult(0).isSuccess shouldBe false
            createManyResult(1).isSuccess shouldBe true
          }
        }
      }
    }

    describe("should delete many documents by query") {
      final case class SetUp(collectionName: String, companyIds: Seq[String])

      def setUpTest(): SetUp = {
        val collectionName = UUID.randomUUID().toString

        val firstDocument = randomDocument().copy(budget = 100, evaluation = 5.6)
        val secondDocument = randomDocument().copy(budget = 200, evaluation = 3.2)
        val thirdDocument = randomDocument().copy(budget = 300, evaluation = 4.3)
        val fourthDocument = randomDocument().copy(budget = 400, evaluation = 2.1)

        val indexFirst = IndexDocument(collectionName, firstDocument)
        val indexSecond = IndexDocument(collectionName, secondDocument)
        val indexThird = IndexDocument(collectionName, thirdDocument)
        val indexFourth = IndexDocument(collectionName, fourthDocument)

        createCollection(collectionName)

        runWithFlow(indexFirst, Typesense.indexDocumentFlow[Company](settings))
        runWithFlow(indexSecond, Typesense.indexDocumentFlow[Company](settings))
        runWithFlow(indexThird, Typesense.indexDocumentFlow[Company](settings))
        runWithFlow(indexFourth, Typesense.indexDocumentFlow[Company](settings))

        SetUp(collectionName, Seq(firstDocument.id, secondDocument.id, thirdDocument.id, fourthDocument.id))
      }

      def testWithFlow(deleteQuery: SetUp => FilterDeleteDocumentsQuery, expectedDeleted: Int): Assertion = {
        //given
        val setUp = setUpTest()
        val collectionName = setUp.collectionName
        val delete = DeleteManyDocumentsByQuery(collectionName, deleteQuery(setUp))

        //when
        val deleteResult = runWithFlow(delete, Typesense.deleteManyDocumentsByQueryFlow(settings))

        //then
        deleteResult shouldBe DeleteManyDocumentsResult(expectedDeleted)
      }

      describe("with field bigger than int") {
        describe("with text query") {
          describe("without batch size") {
            it("using flow") {
              //given
              val setUp = setUpTest()
              val collectionName = setUp.collectionName
              val delete = DeleteManyDocumentsByQuery(collectionName, "budget:>150")

              //when
              val deleteResult = runWithFlow(delete, Typesense.deleteManyDocumentsByQueryFlow(settings))

              //then
              deleteResult shouldBe DeleteManyDocumentsResult(3)
            }

            it("using flow with Java API") {
              //given
              val setUp = setUpTest()
              val collectionName = setUp.collectionName
              val delete = DeleteManyDocumentsByQuery(collectionName, "budget:>150")

              //when
              val deleteResult = runWithJavaFlow(delete, JavaTypesense.deleteManyDocumentsByQueryFlow(settings))

              //then
              deleteResult shouldBe DeleteManyDocumentsResult(3)
            }
          }

          describe("with batch size") {
            it("using flow") {
              //given
              val setUp = setUpTest()
              val collectionName = setUp.collectionName
              val delete = DeleteManyDocumentsByQuery(collectionName, "budget:>150", batchSize = Some(2))

              //when
              val deleteResult = runWithFlow(delete, Typesense.deleteManyDocumentsByQueryFlow(settings))

              //then
              deleteResult shouldBe DeleteManyDocumentsResult(3)
            }

            it("using flow with Java API") {
              //given
              val setUp = setUpTest()
              val collectionName = setUp.collectionName
              val delete = DeleteManyDocumentsByQuery(collectionName, "budget:>150", batchSize = Some(2))

              //when
              val deleteResult = runWithJavaFlow(delete, JavaTypesense.deleteManyDocumentsByQueryFlow(settings))

              //then
              deleteResult shouldBe DeleteManyDocumentsResult(3)
            }
          }
        }

        describe("with DSL") {
          it("using flow") {
            testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.biggerThanInt("budget", 100),
                         expectedDeleted = 3)
          }
        }
      }

      describe("with field bigger than float") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.biggerThanFloat("evaluation", 3.2),
                       expectedDeleted = 2)
        }
      }

      describe("with field bigger than or equal int") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.biggerThanOrEqualInt("budget", 200),
                       expectedDeleted = 3)
        }
      }

      describe("with field bigger than or equal float") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.biggerThanOrEqualFloat("evaluation", 3.2),
                       expectedDeleted = 3)
        }
      }

      describe("with field lower than int") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.lowerThanInt("budget", 200),
                       expectedDeleted = 1)
        }
      }

      describe("with field lower than float") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.lowerThanFloat("evaluation", 3.2),
                       expectedDeleted = 1)
        }
      }

      describe("with field lower than or equal int") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.lowerThanOrEqualInt("budget", 200),
                       expectedDeleted = 2)
        }
      }

      describe("with field lower than or equal float") {
        it("using flow") {
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.lowerThanOrEqualFloat("evaluation", 3.2),
                       expectedDeleted = 2)
        }
      }

      describe("with string field in set") {
        it("using flow") {
          testWithFlow(deleteQuery = setUp => FilterDeleteDocumentsQueryDsl.inStringSet("id", setUp.companyIds.take(2)),
                       expectedDeleted = 2)
        }
      }

      describe("with int field in set") {
        it("using flow") {
          //given
          testWithFlow(deleteQuery = _ => FilterDeleteDocumentsQueryDsl.inIntSet("budget", Seq(200, 400)),
                       expectedDeleted = 2)
        }
      }
    }
  }
}
