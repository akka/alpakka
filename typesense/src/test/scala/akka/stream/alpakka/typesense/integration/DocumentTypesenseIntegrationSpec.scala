/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration

import akka.Done
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.scaladsl.Typesense
import com.dimafeng.testcontainers.DockerComposeContainer
import spray.json.JsonWriter

import java.util.UUID
import scala.jdk.FutureConverters.CompletionStageOps

//all tests run in the same container without cleaning data
abstract class DocumentTypesenseIntegrationSpec(version: String) extends TypesenseIntegrationSpec(version) {
  import DocumentTypesenseIntegrationSpec._

  override def afterContainersStart(containers: DockerComposeContainer): Unit = createCompaniesCollection()

  protected def createCompaniesCollection(): Unit

  describe(s"For Typesense $version") {
    describe("should index single document") {
      describe("with default action") {
        it("using flow") {
          val document = randomDocument()
          val result = runWithFlow(document, Typesense.indexDocumentFlow(settings))
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink") {
          val document = randomDocument()
          val result = runWithSink(document, Typesense.indexDocumentSink(settings))
          result shouldBe Done
        }

        it("using direct request") {
          val document = randomDocument()
          val result = Typesense.indexDocumentRequest(settings, document).futureValue
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using flow with Java API") {
          val document = randomDocument()
          val result = runWithJavaFlow(
            document,
            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]])
          )
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink with Java API") {
          val document = randomDocument()
          val result = runWithJavaSink(
            document,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          result shouldBe Done
        }

        it("using direct request with Java API") {
          val document = randomDocument()
          val result = JavaTypesense
            .indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          result shouldBe Done
          //TODO: compare - retrieve
        }
      }

      describe("with create action") {
        it("using flow") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = runWithFlow(document, Typesense.indexDocumentFlow(settings))
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = runWithSink(document, Typesense.indexDocumentSink(settings))
          result shouldBe Done
        }

        it("using direct request") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = Typesense.indexDocumentRequest(settings, document).futureValue
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using flow with Java API") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = runWithJavaFlow(
            document,
            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]])
          )
          result shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink with Java API") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = runWithJavaSink(
            document,
            JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]])
          )
          result shouldBe Done
        }

        it("using direct request with Java API") {
          val document = randomDocument(IndexDocumentAction.Create)
          val result = JavaTypesense
            .indexDocumentRequest(settings, document, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          result shouldBe Done
          //TODO: compare - retrieve
        }
      }

      describe("with upsert action") {
        def upsertFromCreate(createDocument: IndexDocument[Company]): IndexDocument[Company] =
          createDocument
            .withAction(IndexDocumentAction.Upsert)
            .withContent(createDocument.content.copy(name = "New Name"))

        it("using flow") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult = runWithFlow(createDocument, Typesense.indexDocumentFlow(settings))
          val upsertResult = runWithFlow(upsertDocument, Typesense.indexDocumentFlow(settings))
          createResult shouldBe Done
          upsertResult shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult = runWithSink(createDocument, Typesense.indexDocumentSink(settings))
          val upsertResult = runWithSink(upsertDocument, Typesense.indexDocumentSink(settings))
          createResult shouldBe Done
          upsertResult shouldBe Done
        }

        it("using direct request") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult = Typesense.indexDocumentRequest(settings, createDocument).futureValue
          val upsertResult = Typesense.indexDocumentRequest(settings, upsertDocument).futureValue
          createResult shouldBe Done
          upsertResult shouldBe Done
          //TODO: compare - retrieve
        }

        it("using flow with Java API") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult =
            runWithJavaFlow(createDocument,
                            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]))
          val upsertResult =
            runWithJavaFlow(upsertDocument,
                            JavaTypesense.indexDocumentFlow(settings, implicitly[JsonWriter[Company]]))
          createResult shouldBe Done
          upsertResult shouldBe Done
          //TODO: compare - retrieve
        }

        it("using sink with Java API") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult = runWithJavaSink(createDocument, JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
          val upsertResult = runWithJavaSink(upsertDocument, JavaTypesense.indexDocumentSink(settings, implicitly[JsonWriter[Company]]))
          createResult shouldBe Done
          upsertResult shouldBe Done
        }

        it("using direct request with Java API") {
          val createDocument = randomDocument(IndexDocumentAction.Create)
          val upsertDocument = upsertFromCreate(createDocument)
          val createResult = JavaTypesense
            .indexDocumentRequest(settings, createDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          val upsertResult = JavaTypesense
            .indexDocumentRequest(settings, upsertDocument, system, implicitly[JsonWriter[Company]])
            .asScala
            .futureValue
          createResult shouldBe Done
          upsertResult shouldBe Done
          //TODO: compare - retrieve
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
  }

  private def randomDocument(): IndexDocument[Company] =
    IndexDocument("companies", Company(UUID.randomUUID().toString, "Functional Corporation", 1000))

  private def randomDocument(action: IndexDocumentAction): IndexDocument[Company] =
    IndexDocument("companies", Company(UUID.randomUUID().toString, "Functional Corporation", 1000), action)
}

private[integration] object DocumentTypesenseIntegrationSpec {
  import spray.json._
  import DefaultJsonProtocol._
  final case class Company(id: String, name: String, budget: Int)
  implicit val companyFormat: RootJsonFormat[Company] = jsonFormat3(Company)
}
