/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.scaladsl.Typesense
import com.dimafeng.testcontainers.DockerComposeContainer
import spray.json.{JsonReader, JsonWriter}

import java.util.UUID

//all tests run in the same container without cleaning data
abstract class DocumentTypesenseIntegrationSpec(version: String) extends TypesenseIntegrationSpec(version) {
  import DocumentTypesenseIntegrationSpec._

  override def afterContainersStart(containers: DockerComposeContainer): Unit = createCollection("companies")

  protected def createCollection(name: String): Unit

  describe(s"For Typesense $version") {
    describe("should index and retrieve single document") {
      describe("with default action") {
        it("using flow") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          val retrieveResult = runWithFlowTypesenseResult(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using flow with Java API") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaFlowTypesenseResult(
            indexDocument,
            JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult =
            runWithJavaFlowTypesenseResult(retrieve,
                                           JavaTypesense.retrieveDocumentFlow[Company](settings,
                                                                                       implicitly[JsonReader[Company]]))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }
      }

      describe("with create action") {
        it("using flow") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          val retrieveResult = runWithFlowTypesenseResult(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using flow with Java API") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaFlowTypesenseResult(
            indexDocument,
            JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = runWithJavaFlowTypesenseResult(
            retrieve,
            JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]])
          )

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }
      }
    }

    describe("should not index single document") {
      describe("with default action") {
        describe("if collection doesn't exist") {
          val document =
            IndexDocument("wrong-collection", Company(UUID.randomUUID().toString, "Functional Corporation", 1000))
          it("using flow") {
            tryUsingFlowAndExpectError(document, Typesense.indexDocumentFlow[Company](settings), StatusCodes.NotFound)
          }

          it("using flow with Java API") {
            tryUsingJavaFlowAndExpectError(document,
                                           JavaTypesense.indexDocumentFlow[Company](settings,
                                                                                    implicitly[JsonWriter[Company]]),
                                           StatusCodes.NotFound)
          }
        }

        it("if document with this id already exists") {
          val indexDocument = randomIndexDocument()
          runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          tryUsingFlowAndExpectError(indexDocument,
                                     Typesense.indexDocumentFlow[Company](settings),
                                     StatusCodes.Conflict)
        }
      }

      describe("with create action") {
        describe("if document with this id already exists") {
          it("using flow") {
            val indexDocument = randomIndexDocument()
            runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingFlowAndExpectError(indexDocument,
                                       Typesense.indexDocumentFlow[Company](settings),
                                       StatusCodes.Conflict)
          }

          it("using flow with Java API") {
            val indexDocument = randomIndexDocument()
            runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingJavaFlowAndExpectError(indexDocument,
                                           JavaTypesense.indexDocumentFlow[Company](settings,
                                                                                    implicitly[JsonWriter[Company]]),
                                           StatusCodes.Conflict)
          }
        }
      }
    }

    describe("should not retrieve document") {
      describe("if doesn't exist") {
        val retrieve = RetrieveDocument("companies", UUID.randomUUID().toString)

        it("using flow") {
          tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow[Company](settings), StatusCodes.NotFound)
        }

        it("using flow with Java API") {
          tryUsingJavaFlowAndExpectError(retrieve,
                                         JavaTypesense.retrieveDocumentFlow[Company](settings,
                                                                                     implicitly[JsonReader[Company]]),
                                         StatusCodes.NotFound)

        }
      }

      describe("if collection doesn't exist") {
        val retrieve = RetrieveDocument("invalid-collection", UUID.randomUUID().toString)

        it("using flow") {
          tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow[Company](settings), StatusCodes.NotFound)
        }
      }
    }

    describe("should delete document") {
      it("using flow") {
        val indexDocument = randomIndexDocument()
        val delete = deleteDocumentFromIndexDocument(indexDocument)
        val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

        //when
        val createResult = runWithFlowTypesenseResult(indexDocument, Typesense.indexDocumentFlow[Company](settings))
        val deleteResult = runWithFlowTypesenseResult(delete, Typesense.deleteDocumentFlow(settings))

        //then
        createResult shouldBe Done
        deleteResult shouldBe Done
        tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow[Company](settings), StatusCodes.NotFound)
      }

      it("using flow with Java API") {
        val indexDocument = randomIndexDocument()
        val delete = deleteDocumentFromIndexDocument(indexDocument)
        val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

        //when
        val createResult =
          runWithJavaFlowTypesenseResult(indexDocument,
                                         JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]))
        val deleteResult = runWithJavaFlowTypesenseResult(delete, JavaTypesense.deleteDocumentFlow(settings))

        //then
        createResult shouldBe Done
        deleteResult shouldBe Done
        tryUsingJavaFlowAndExpectError(retrieve,
                                       JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]]),
                                       StatusCodes.NotFound)
      }
    }

    describe("should not delete document") {
      describe("if doesn't exist") {
        val deleteNonexistentDocument = DeleteDocument("companies", UUID.randomUUID().toString)

        it("using flow") {
          tryUsingFlowAndExpectError(deleteNonexistentDocument,
                                     Typesense.deleteDocumentFlow(settings),
                                     StatusCodes.NotFound)
        }

        it("using flow with Java API") {
          tryUsingJavaFlowAndExpectError(deleteNonexistentDocument,
                                         JavaTypesense.deleteDocumentFlow(settings),
                                         StatusCodes.NotFound)
        }
      }
    }
  }

  protected def randomIndexDocument(): IndexDocument[Company] =
    IndexDocument("companies", randomDocument())

  protected def randomIndexDocument(action: IndexDocumentAction): IndexDocument[Company] =
    IndexDocument("companies", randomDocument(), action)

  protected def randomDocument(): Company = Company(UUID.randomUUID().toString, "Functional Corporation", 1000)

  protected def retrieveDocumentFromIndexDocument(indexDocument: IndexDocument[Company]): RetrieveDocument =
    RetrieveDocument(indexDocument.collectionName, indexDocument.content.id)

  protected def deleteDocumentFromIndexDocument(indexDocument: IndexDocument[Company]): DeleteDocument =
    DeleteDocument(indexDocument.collectionName, indexDocument.content.id)
}

private[integration] object DocumentTypesenseIntegrationSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String, budget: Int, evaluation: Double = 4.3)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat4(Company)
  final case class UpdateCompany(id: String, budget: Int)
  implicit val updateCompanyFormat: RootJsonFormat[UpdateCompany] = jsonFormat2(UpdateCompany)
}
