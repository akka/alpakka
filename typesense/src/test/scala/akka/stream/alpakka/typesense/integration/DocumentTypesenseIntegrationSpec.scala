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
import scala.jdk.FutureConverters.CompletionStageOps

//all tests run in the same container without cleaning data
abstract class DocumentTypesenseIntegrationSpec(version: String) extends TypesenseIntegrationSpec(version) {
  import DocumentTypesenseIntegrationSpec._

  override def afterContainersStart(containers: DockerComposeContainer): Unit = createCompaniesCollection()

  protected def createCompaniesCollection(): Unit

  describe(s"For Typesense $version") {
    describe("should index and retrieve single document") {
      describe("with default action") {
        it("using flow") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using sink") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithSink(indexDocument, Typesense.indexDocumentSink[Company](settings))
          val retrieveResult = Typesense.retrieveDocumentRequest[Company](settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using direct request") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = Typesense.indexDocumentRequest(settings, indexDocument).futureValue
          val retrieveResult = Typesense.retrieveDocumentRequest[Company](settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using flow with Java API") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaFlow(
            indexDocument,
            JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult =
            runWithJavaFlow(retrieve,
                            JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]]))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using sink with Java API") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaSink(
            indexDocument,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = Typesense.retrieveDocumentRequest[Company](settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using direct request with Java API") {
          //given
          val indexDocument = randomIndexDocument()
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, indexDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val retrieveResult = JavaTypesense
            .retrieveDocumentRequest[Company](settings, retrieve, system, implicitly[JsonReader[Company]])
            .asScala
            .futureValue

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
          val createResult = runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using sink") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithSink(indexDocument, Typesense.indexDocumentSink[Company](settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow[Company](settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using direct request") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = Typesense.indexDocumentRequest(settings, indexDocument).futureValue
          val retrieveResult = Typesense.retrieveDocumentRequest[Company](settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using flow with Java API") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaFlow(
            indexDocument,
            JavaTypesense.indexDocumentFlow[Company](settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = runWithJavaFlow(
            retrieve,
            JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]])
          )

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using sink with Java API") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = runWithJavaSink(
            indexDocument,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = runWithJavaFlow(
            retrieve,
            JavaTypesense.retrieveDocumentFlow[Company](settings, implicitly[JsonReader[Company]])
          )

          //then
          createResult shouldBe Done
          retrieveResult shouldBe indexDocument.content
        }

        it("using direct request with Java API") {
          //given
          val indexDocument = randomIndexDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(indexDocument)

          //when
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, indexDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val retrieveResult = JavaTypesense
            .retrieveDocumentRequest[Company](settings, retrieve, system, implicitly[JsonReader[Company]])
            .asScala
            .futureValue

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

          it("using sink") {
            tryUsingSinkAndExpectError(document, Typesense.indexDocumentSink[Company](settings), StatusCodes.NotFound)
          }

          it("using direct request") {
            tryUsingDirectRequestAndExpectError(Typesense.indexDocumentRequest(settings, document),
                                                StatusCodes.NotFound)
          }

          it("using flow with Java API") {
            tryUsingJavaFlowAndExpectError(document,
                                           JavaTypesense.indexDocumentFlow[Company](settings,
                                                                                    implicitly[JsonWriter[Company]]),
                                           StatusCodes.NotFound)
          }

          it("using sink with Java API") {
            tryUsingJavaSinkAndExpectError(document,
                                           JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]),
                                           StatusCodes.NotFound)
          }

          it("using direct request with Java API") {
            tryUsingJavaDirectRequestAndExpectError(
              JavaTypesense.indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]]),
              StatusCodes.NotFound
            )
          }
        }

        it("if document with this id already exists") {
          val indexDocument = randomIndexDocument()
          runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
          tryUsingFlowAndExpectError(indexDocument,
                                     Typesense.indexDocumentFlow[Company](settings),
                                     StatusCodes.Conflict)
        }
      }

      describe("with create action") {
        describe("if document with this id already exists") {
          it("using flow") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingFlowAndExpectError(indexDocument,
                                       Typesense.indexDocumentFlow[Company](settings),
                                       StatusCodes.Conflict)
          }

          it("using sink") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingSinkAndExpectError(indexDocument,
                                       Typesense.indexDocumentSink[Company](settings),
                                       StatusCodes.Conflict)
          }

          it("using direct request") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingDirectRequestAndExpectError(Typesense.indexDocumentRequest(settings, indexDocument),
                                                StatusCodes.Conflict)
          }

          it("using flow with Java API") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingJavaFlowAndExpectError(indexDocument,
                                           JavaTypesense.indexDocumentFlow[Company](settings,
                                                                                    implicitly[JsonWriter[Company]]),
                                           StatusCodes.Conflict)
          }

          it("using sink with Java API") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingJavaSinkAndExpectError(indexDocument,
                                           JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]),
                                           StatusCodes.Conflict)
          }

          it("using direct request with Java API") {
            val indexDocument = randomIndexDocument()
            runWithFlow(indexDocument, Typesense.indexDocumentFlow[Company](settings))
            tryUsingJavaDirectRequestAndExpectError(
              JavaTypesense.indexDocumentRequest(settings, indexDocument, system, implicitly[JsonWriter[Company]]),
              StatusCodes.Conflict
            )
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

        it("using direct request") {
          tryUsingDirectRequestAndExpectError(Typesense.retrieveDocumentRequest[Company](settings, retrieve),
                                              StatusCodes.NotFound)
        }

        it("using flow with Java API") {
          tryUsingJavaFlowAndExpectError(retrieve,
                                         JavaTypesense.retrieveDocumentFlow[Company](settings,
                                                                                     implicitly[JsonReader[Company]]),
                                         StatusCodes.NotFound)

        }

        it("using direct request with Java API") {
          tryUsingJavaDirectRequestAndExpectError(
            JavaTypesense.retrieveDocumentRequest[Company](settings, retrieve, system, implicitly[JsonReader[Company]]),
            StatusCodes.NotFound
          )
        }
      }

      describe("if collection doesn't exist") {
        val retrieve = RetrieveDocument("invalid-collection", UUID.randomUUID().toString)

        it("using flow") {
          tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow[Company](settings), StatusCodes.NotFound)
        }
      }
    }
  }

  protected def randomIndexDocument(): IndexDocument[Company] =
    IndexDocument("companies", randomDocument())

  protected def randomIndexDocument(action: IndexDocumentAction): IndexDocument[Company] =
    IndexDocument("companies", randomDocument(), action)

  protected def randomDocument(): Company = Company(UUID.randomUUID().toString, "Functional Corporation", 1000)

  protected def retrieveDocumentFromIndexDocument(indexDocument: IndexDocument[Company]) =
    RetrieveDocument(indexDocument.collectionName, indexDocument.content.id)
}

private[integration] object DocumentTypesenseIntegrationSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String, budget: Int)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat3(Company)
  final case class UpdateCompany(id: String, budget: Int)
  implicit val updateCompanyFormat: RootJsonFormat[UpdateCompany] = jsonFormat2(UpdateCompany)

}
