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
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithFlow(document, Typesense.indexDocumentFlow(settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow(settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using sink") {
          //given
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithSink(document, Typesense.indexDocumentSink(settings))
          val retrieveResult = Typesense.retrieveDocumentRequest(settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using direct request") {
          //given
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = Typesense.indexDocumentRequest(settings, document).futureValue
          val retrieveResult = Typesense.retrieveDocumentRequest(settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using flow with Java API") {
          //given
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithJavaFlow(
            document,
            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult =
            runWithJavaFlow(retrieve, JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]]))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using sink with Java API") {
          //given
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithJavaSink(
            document,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = Typesense.retrieveDocumentRequest(settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using direct request with Java API") {
          //given
          val document = randomDocument()
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val retrieveResult = JavaTypesense
            .retrieveDocumentRequest(settings, retrieve, system, implicitly[JsonReader[Company]])
            .asScala
            .futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }
      }

      describe("with create action") {
        it("using flow") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithFlow(document, Typesense.indexDocumentFlow(settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow(settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using sink") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithSink(document, Typesense.indexDocumentSink(settings))
          val retrieveResult = runWithFlow(retrieve, Typesense.retrieveDocumentFlow(settings))

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using direct request") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = Typesense.indexDocumentRequest(settings, document).futureValue
          val retrieveResult = Typesense.retrieveDocumentRequest(settings, retrieve).futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using flow with Java API") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithJavaFlow(
            document,
            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = runWithJavaFlow(
            retrieve,
            JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]])
          )

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using sink with Java API") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = runWithJavaSink(
            document,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          val retrieveResult = runWithJavaFlow(
            retrieve,
            JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]])
          )

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }

        it("using direct request with Java API") {
          //given
          val document = randomDocument(IndexDocumentAction.Create)
          val retrieve = retrieveDocumentFromIndexDocument(document)

          //when
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val retrieveResult = JavaTypesense
            .retrieveDocumentRequest(settings, retrieve, system, implicitly[JsonReader[Company]])
            .asScala
            .futureValue

          //then
          createResult shouldBe Done
          retrieveResult shouldBe document.content
        }
      }
    }

    describe("should not index single document") {
      describe("with default action") {
        describe("if collection doesn't exist") {
          val document =
            IndexDocument("wrong-collection", Company(UUID.randomUUID().toString, "Functional Corporation", 1000))
          it("using flow") {
            tryUsingFlowAndExpectError(document, Typesense.indexDocumentFlow(settings), StatusCodes.NotFound)
          }

          it("using sink") {
            tryUsingSinkAndExpectError(document, Typesense.indexDocumentSink(settings), StatusCodes.NotFound)
          }

          it("using direct request") {
            tryUsingDirectRequestAndExpectError(Typesense.indexDocumentRequest(settings, document),
                                                StatusCodes.NotFound)
          }

          it("using flow with Java API") {
            tryUsingJavaFlowAndExpectError(document,
                                           JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]),
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
          val document = randomDocument()
          runWithFlow(document, Typesense.indexDocumentFlow(settings))
          tryUsingFlowAndExpectError(document, Typesense.indexDocumentFlow(settings), StatusCodes.Conflict)
        }
      }

      describe("with create action") {
        describe("if document with this id already exists") {
          it("using flow") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingFlowAndExpectError(document, Typesense.indexDocumentFlow(settings), StatusCodes.Conflict)
          }

          it("using sink") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingSinkAndExpectError(document, Typesense.indexDocumentSink(settings), StatusCodes.Conflict)
          }

          it("using direct request") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingDirectRequestAndExpectError(Typesense.indexDocumentRequest(settings, document),
                                                StatusCodes.Conflict)
          }

          it("using flow with Java API") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingJavaFlowAndExpectError(document,
                                           JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]),
                                           StatusCodes.Conflict)
          }

          it("using sink with Java API") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingJavaSinkAndExpectError(document,
                                           JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]),
                                           StatusCodes.Conflict)
          }

          it("using direct request with Java API") {
            val document = randomDocument()
            runWithFlow(document, Typesense.indexDocumentFlow(settings))
            tryUsingJavaDirectRequestAndExpectError(
              JavaTypesense.indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]]),
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
          tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow(settings), StatusCodes.NotFound)
        }

        it("using direct request") {
          tryUsingDirectRequestAndExpectError(Typesense.retrieveDocumentRequest(settings, retrieve),
                                              StatusCodes.NotFound)
        }

        it("using flow with Java API") {
          tryUsingJavaFlowAndExpectError(retrieve,
                                         JavaTypesense.retrieveDocumentFlow(settings, implicitly[JsonReader[Company]]),
                                         StatusCodes.NotFound)

        }

        it("using direct request with Java API") {
          tryUsingJavaDirectRequestAndExpectError(
            JavaTypesense.retrieveDocumentRequest(settings, retrieve, system, implicitly[JsonReader[Company]]),
            StatusCodes.NotFound
          )
        }
      }

      describe("if collection doesn't exist") {
        val retrieve = RetrieveDocument("invalid-collection", UUID.randomUUID().toString)

        it("using flow") {
          tryUsingFlowAndExpectError(retrieve, Typesense.retrieveDocumentFlow(settings), StatusCodes.NotFound)
        }
      }
    }
  }

  protected def randomDocument(): IndexDocument[Company] =
    IndexDocument("companies", Company(UUID.randomUUID().toString, "Functional Corporation", 1000))

  protected def randomDocument(action: IndexDocumentAction): IndexDocument[Company] =
    IndexDocument("companies", Company(UUID.randomUUID().toString, "Functional Corporation", 1000), action)

  protected def retrieveDocumentFromIndexDocument(indexDocument: IndexDocument[Company]) =
    RetrieveDocument(indexDocument.collectionName, indexDocument.content.id)
}

private[integration] object DocumentTypesenseIntegrationSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String, budget: Int)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat3(Company)
}
