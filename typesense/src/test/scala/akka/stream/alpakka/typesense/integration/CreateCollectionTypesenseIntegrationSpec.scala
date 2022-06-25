package akka.stream.alpakka.typesense.integration

import akka.Done
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.alpakka.typesense._
import akka.stream.alpakka.typesense.impl.TypesenseHttp.TypesenseException
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.Assertion

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

abstract class CreateCollectionTypesenseIntegrationSpec(val version: String) extends TypesenseIntegrationSpec(version) {
  import system.dispatcher
  val mockedTime: Instant = Instant.now()

  //all tests run in the same container without cleaning data

  val defaultFields = Seq(Field("name", FieldType.String))

  describe(s"For Typesense $version") {
    describe("should create collection") {
      it("using flow") {
        createAndCheck(randomSchema())
      }

      it("using sink") {
        val schema = randomSchema()
        val result = Source
          .single(schema)
          .toMat(Typesense.createCollectionSink(settings))(Keep.right)
          .run()
          .futureValue

        result shouldBe Done
      }

      it("using direct request") {
        val schema = randomSchema()
        val result = Typesense.createCollectionRequest(settings, schema).futureValue
        compareResponseAndSchema(result, schema)
      }

      it("using flow with Java API") {
        val schema = randomSchema()
        val result: Future[CollectionResponse] = Source
          .single(schema)
          .via(akka.stream.alpakka.typesense.javadsl.Typesense.createCollectionFlow(settings))
          .toMat(Sink.head)(Keep.right)
          .run()

        compareResponseAndSchema(result.futureValue, schema)
      }

      it("using sink with Java API") {
        val schema = randomSchema()
        val result: CompletionStage[Done] = Source
          .single(schema)
          .toMat(akka.stream.alpakka.typesense.javadsl.Typesense.createCollectionSink(settings))(Keep.right)
          .run()

        result.asScala.futureValue shouldBe Done
      }

      it("using direct request with Java API") {
        val schema = randomSchema()
        val result: CompletionStage[CollectionResponse] = akka.stream.alpakka.typesense.javadsl.Typesense
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

      it("with specified correct default sorting field") {
        val fields = Seq(Field("company_nr", FieldType.Int32))
        createAndCheck(randomSchema(fields).withDefaultSortingField("company_nr"))
      }

      it("with specified empty default sorting field") {
        createAndCheck(randomSchema().withDefaultSortingField(""))
      }

      it("with string field") {
        val field = Field("name", FieldType.String)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with string[] field") {
        val field = Field("names", FieldType.StringArray)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with int32 field") {
        val field = Field("company_nr", FieldType.Int32)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with int32[] field") {
        val field = Field("company_nrs", FieldType.Int32Array)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with int64 field") {
        val field = Field("company_nr", FieldType.Int64)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with int64[] field") {
        val field = Field("company_nrs", FieldType.Int64Array)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with float field") {
        val field = Field("company_nr", FieldType.Int64)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with float[] field") {
        val field = Field("company_nrs", FieldType.Int64Array)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with bool field") {
        val field = Field("active", FieldType.Bool)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with bool[] field") {
        val field = Field("active", FieldType.BoolArray)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with geopoint field") {
        val field = Field("geo", FieldType.Geopoint)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with geopoint[] field") {
        val field = Field("geo", FieldType.GeopointArray)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with string* field") {
        val field = Field("names", FieldType.StringAutoArray)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with auto field") {
        val field = Field("data", FieldType.Auto)
        createAndCheck(randomSchema(fields = Seq(field)))
      }

      it("with many fields") {
        val fields = Seq(Field("first-field", FieldType.String), Field("second-field", FieldType.String))
        createAndCheck(randomSchema(fields = fields))
      }
    }

    describe("should not create collection") {
      it("with invalid default sorting field") {
        val fields = Seq(Field("company_nr", FieldType.Int32))
        val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")
        tryCreateAndExpectError(schema, expectedstatusCode = StatusCodes.BadRequest)
      }

      it("if already exist") {
        val schema = randomSchema()
        createAndCheck(schema)
        tryCreateAndExpectError(schema, expectedstatusCode = StatusCodes.Conflict)
      }

      it("if is invalid using sink") {
        val fields = Seq(Field("company_nr", FieldType.Int32))
        val schema = randomSchema(fields = fields).withDefaultSortingField("invalid")

        val result = Source
          .single(schema)
          .toMat(Typesense.createCollectionSink(settings))(Keep.right)
          .run()
          .map(Success.apply)
          .recover(e => Failure(e))
          .futureValue

        val gotStatusCode = result.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode

        gotStatusCode shouldBe StatusCodes.BadRequest
      }
    }
  }

  private def randomSchema(fields: Seq[Field] = defaultFields): CollectionSchema =
    CollectionSchema("my-collection-" + UUID.randomUUID(), fields)

  private def createAndCheck(schema: CollectionSchema): Assertion = {
    val response = Source
      .single(schema)
      .via(Typesense.createCollectionFlow(settings))
      .toMat(Sink.head)(Keep.right)
      .run()
      .futureValue
    compareResponseAndSchema(response, schema)
  }

  private def tryCreateAndExpectError(schema: CollectionSchema, expectedstatusCode: StatusCode): Assertion = {
    val response = Source
      .single(schema)
      .via(Typesense.createCollectionFlow(settings))
      .map(Success.apply)
      .recover(e => Failure(e))
      .toMat(Sink.head)(Keep.right)
      .run()
      .futureValue

    val gotStatusCode = response.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode

    gotStatusCode shouldBe expectedstatusCode
  }

  private def compareResponseAndSchema(response: CollectionResponse, schema: CollectionSchema): Assertion = {
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

  private def fieldResponseFromField(field: Field): FieldResponse = {
    val autoOptionalTypes: Set[FieldType] = Set(FieldType.StringAutoArray, FieldType.Auto)
    val optional = if (autoOptionalTypes.contains(field.`type`)) true else field.optional.getOrElse(false)
    val facet = field.facet.getOrElse(false)
    val index = field.facet.getOrElse(true)

    FieldResponse(name = field.name, `type` = field.`type`, optional = optional, facet = facet, index = index)
  }
}
