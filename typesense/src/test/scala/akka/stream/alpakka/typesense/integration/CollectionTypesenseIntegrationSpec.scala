/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration

import akka.Done
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.Assertion

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.jdk.FutureConverters.CompletionStageOps

//all tests run in the same container without cleaning data
abstract class CollectionTypesenseIntegrationSpec(version: String) extends TypesenseIntegrationSpec(version) {
  protected val mockedTime: Instant = Instant.now()

  protected val defaultSortingFieldName: String = "item-nr"
  protected val defaultSortingField: Field = Field(defaultSortingFieldName, FieldType.Int32)
  protected val defaultFields: Seq[Field] = Seq(defaultSortingField, Field("name", FieldType.String))

  describe(s"For Typesense $version") {
    describe("should create collection") {
      it("using flow") {
        createAndCheck(randomSchema())
      }

      it("using sink") {
        val schema = randomSchema()
        val result = runWithSink(schema, Typesense.createCollectionSink(settings))

        result shouldBe Done
      }

      it("using direct request") {
        val schema = randomSchema()
        val result = Typesense.createCollectionRequest(settings, schema).futureValue
        compareResponseAndSchema(result, schema)
      }

      it("using flow with Java API") {
        val schema = randomSchema()
        val result = runWithJavaFlow(schema, JavaTypesense.createCollectionFlow(settings))

        compareResponseAndSchema(result, schema)
      }

      it("using sink with Java API") {
        val schema = randomSchema()
        val result = runWithJavaSink(schema, JavaTypesense.createCollectionSink(settings))

        result shouldBe Done
      }

      it("using direct request with Java API") {
        val schema = randomSchema()
        val result: CompletionStage[CollectionResponse] = JavaTypesense
          .createCollectionRequest(settings, schema, system)

        compareResponseAndSchema(result.asScala.futureValue, schema)
      }

      it("with specified token separators") {
        createAndCheck(randomSchema().withTokenSeparators(Seq("-")))
      }

      it("with specified empty token separators") {
        createAndCheck(randomSchema().withTokenSeparators(Seq.empty))
      }

      it("with specified symbols to index") {
        createAndCheck(randomSchema().withSymbolsToIndex(Seq("+")))
      }

      it("with specified empty symbols to index") {
        createAndCheck(randomSchema().withSymbolsToIndex(Seq.empty))
      }

      describe("with field") {

        def testWithField(field: Field): Unit = {
          val fields = Seq(defaultSortingField, field)
          createAndCheck(randomSchema(fields = fields).withDefaultSortingField(defaultSortingFieldName))
        }

        it("string") {
          val field = Field("name", FieldType.String)
          testWithField(field)
        }

        it("string[]") {
          val field = Field("names", FieldType.StringArray)
          testWithField(field)
        }

        it("int32") {
          val field = Field("company_nr", FieldType.Int32)
          testWithField(field)
        }

        it("int32[]") {
          val field = Field("company_nrs", FieldType.Int32Array)
          testWithField(field)
        }

        it("int64") {
          val field = Field("company_nr", FieldType.Int64)
          testWithField(field)
        }

        it("int64[]") {
          val field = Field("company_nrs", FieldType.Int64Array)
          testWithField(field)
        }

        it("float") {
          val field = Field("company_nr", FieldType.Int64)
          testWithField(field)
        }

        it("float[]") {
          val field = Field("company_nrs", FieldType.Int64Array)
          testWithField(field)
        }

        it("bool") {
          val field = Field("active", FieldType.Bool)
          testWithField(field)
        }

        it("bool[]") {
          val field = Field("active", FieldType.BoolArray)
          testWithField(field)
        }

        it("geopoint") {
          val field = Field("geo", FieldType.Geopoint)
          testWithField(field)
        }

        it("geopoint[]") {
          val field = Field("geo", FieldType.GeopointArray)
          testWithField(field)
        }

        it("string*") {
          val field = Field("names", FieldType.StringAutoArray)
          testWithField(field)
        }

        it("auto") {
          val field = Field("data", FieldType.Auto)
          testWithField(field)
        }
      }
    }

    describe("should not create collection") {
      describe("with invalid default sorting field") {
        it("using flow") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")
          tryCreateAndExpectError(schema, expectedStatusCode = StatusCodes.BadRequest)
        }

        it("using sink") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

          tryUsingSinkAndExpectError(schema, Typesense.createCollectionSink(settings), StatusCodes.BadRequest)
        }

        it("using direct request") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

          tryUsingDirectRequestAndExpectError(Typesense.createCollectionRequest(settings, schema),
                                              StatusCodes.BadRequest)
        }

        it("using flow with Java API") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

          tryUsingJavaFlowAndExpectError(schema, JavaTypesense.createCollectionFlow(settings), StatusCodes.BadRequest)
        }

        it("using sink with Java API") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

          tryUsingJavaSinkAndExpectError(schema, JavaTypesense.createCollectionSink(settings), StatusCodes.BadRequest)
        }

        it("using direct request with Java API") {
          val fields = Seq(Field("company_nr", FieldType.Int32))
          val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

          tryUsingJavaDirectRequestAndExpectError(
            JavaTypesense.createCollectionRequest(settings, schema, system),
            StatusCodes.BadRequest
          )
        }
      }

      it("if already exists") {
        val schema = randomSchema()
        createAndCheck(schema)
        tryCreateAndExpectError(schema, expectedStatusCode = StatusCodes.Conflict)
      }
    }

    describe("should retrieve collection") {
      describe("if exists") {
        it("using flow") {
          val schema = randomSchema()
          val createResult: CollectionResponse = Typesense.createCollectionRequest(settings, schema).futureValue
          val retrieveResult: CollectionResponse = Source
            .single(RetrieveCollection(schema.name))
            .via(Typesense.retrieveCollectionFlow(settings))
            .runWith(Sink.head)
            .futureValue

          createResult shouldBe retrieveResult
        }

        it("using direct request") {
          val schema = randomSchema()
          val createResult: CollectionResponse = Typesense.createCollectionRequest(settings, schema).futureValue
          val retrieveResult: CollectionResponse =
            Typesense.retrieveCollectionRequest(settings, RetrieveCollection(schema.name)).futureValue

          createResult shouldBe retrieveResult
        }

        it("using flow with Java API") {
          val schema = randomSchema()
          val createResult: CollectionResponse = Typesense.createCollectionRequest(settings, schema).futureValue
          val retrieveResult: Future[CollectionResponse] = Source
            .single(RetrieveCollection(schema.name))
            .via(JavaTypesense.retrieveCollectionFlow(settings))
            .runWith(Sink.head)

          createResult shouldBe retrieveResult.futureValue
        }

        it("using direct request with Java API") {
          val schema = randomSchema()
          val createResult: CollectionResponse = Typesense.createCollectionRequest(settings, schema).futureValue
          val retrieveResult: CompletionStage[CollectionResponse] = JavaTypesense
            .retrieveCollectionRequest(settings, RetrieveCollection(schema.name), system)

          createResult shouldBe retrieveResult.asScala.futureValue
        }
      }
    }

    describe("should not retrieve collection") {
      describe("if not exists") {
        val retrieveNonExistingCollection = RetrieveCollection("non-existing-collection")

        it("using flow") {
          tryUsingFlowAndExpectError(retrieveNonExistingCollection,
                                     Typesense.retrieveCollectionFlow(settings),
                                     StatusCodes.NotFound)
        }

        it("using direct request") {
          tryUsingDirectRequestAndExpectError(Typesense.retrieveCollectionRequest(settings,
                                                                                  retrieveNonExistingCollection),
                                              StatusCodes.NotFound)
        }

        it("using flow with Java API") {
          tryUsingJavaFlowAndExpectError(retrieveNonExistingCollection,
                                         JavaTypesense.retrieveCollectionFlow(settings),
                                         StatusCodes.NotFound)
        }

        it("using direct request with Java API") {
          tryUsingJavaDirectRequestAndExpectError(JavaTypesense.retrieveCollectionRequest(settings,
                                                                                          retrieveNonExistingCollection,
                                                                                          system),
                                                  StatusCodes.NotFound)
        }
      }
    }
  }

  protected def randomSchema(): CollectionSchema =
    CollectionSchema("my-collection-" + UUID.randomUUID(), defaultFields)
      .withDefaultSortingField(defaultSortingFieldName)

  protected def randomSchema(fields: Seq[Field]): CollectionSchema =
    CollectionSchema("my-collection-" + UUID.randomUUID(), fields)

  protected def createAndCheck(schema: CollectionSchema): Assertion = {
    val response = runWithFlow(schema, Typesense.createCollectionFlow(settings))
    compareResponseAndSchema(response, schema)
  }

  protected def tryCreateAndExpectError(schema: CollectionSchema, expectedStatusCode: StatusCode): Assertion =
    tryUsingFlowAndExpectError(schema, Typesense.createCollectionFlow(settings), expectedStatusCode)

  protected def compareResponseAndSchema(response: CollectionResponse, schema: CollectionSchema): Assertion = {
    val expectedResponse = CollectionResponse(
      name = schema.name,
      numDocuments = 0,
      fields = schema.fields.map(fieldResponseFromField),
      defaultSortingField = schema.defaultSortingField.getOrElse(""),
      createdAt = mockedTime
    )

    val responseToCheck = CollectionResponse(
      name = response.name,
      numDocuments = response.numDocuments,
      fields = response.fields,
      defaultSortingField = response.defaultSortingField,
      createdAt = mockedTime
    )

    expectedResponse shouldBe responseToCheck
  }

  protected def fieldResponseFromField(field: Field): FieldResponse
}
