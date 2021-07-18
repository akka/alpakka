/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.storage.impl.{AvroDecoder, SimpleRowReader}
import akka.stream.alpakka.googlecloud.bigquery.storage.{
  BigQueryRecord,
  BigQueryStorageSettings,
  BigQueryStorageSpecBase
}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}
import com.google.cloud.bigquery.storage.v1.avro.{AvroRows, AvroSchema}
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BigQueryStorageSpec
    extends BigQueryStorageSpecBase(21001)
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {

  "GoogleBigQuery.readMergedStreams" should {
    "stream the results for a query" in {
      val seq = BigQueryStorage
        .createMergedStreams(Project, Dataset, Table, DataFormat.AVRO, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      val avroSchema = storageAvroSchema
      val avroRows = storageAvroRows

      seq shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)((avroSchema, avroRows))
    }
  }

  "GoogleBigQuery.read" should {
    "filter results based on the row restriction configured" in {
      implicit val um: AvroByteStringDecoder = new AvroByteStringDecoder(FullAvroSchema)

      BigQueryStorage
        .typed[List[BigQueryRecord]](Project,
                                     Dataset,
                                     Table,
                                     DataFormat.AVRO,
                                     Some(TableReadOptions(rowRestriction = "true = false")))
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "stream the results for a query using avro deserializer" in {
      val avroSchema = storageAvroSchema.value
      val avroRows = storageAvroRows.value

      val decoder = AvroDecoder(avroSchema.schema)
      val records = decoder.decodeRows(avroRows.serializedBinaryRows).map(gr => BigQueryRecord.fromAvro(gr))

      implicit val um: AvroByteStringDecoder = new AvroByteStringDecoder(FullAvroSchema)

      val seq = BigQueryStorage
        .typed[List[BigQueryRecord]](Project, Dataset, Table, DataFormat.AVRO, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      seq shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)(records)
    }

    "restrict the number of Avro streams/Sources returned by the Storage API, if specified" in {
      val maxStreams: Int = 5

      BigQueryStorage
        .create(Project, Dataset, Table, DataFormat.AVRO, None, maxNumStreams = maxStreams)
        .withAttributes(mockBQReader())
        .map(_._2.size)
        .runFold(0)(_ + _)
        .futureValue shouldBe maxStreams
    }

    "apply configured column restrictions" in {
      val avroSchema = AvroSchema(Col1Schema.toString)
      val avroRows = recordsAsRows(Col1AvroRecord)

      val decoder = AvroDecoder(avroSchema.schema)
      val records = decoder.decodeRows(avroRows.serializedBinaryRows).map(gr => BigQueryRecord.fromAvro(gr))

      implicit val um: AvroByteStringDecoder = new AvroByteStringDecoder(Col1Schema)

      BigQueryStorage
        .typed[List[BigQueryRecord]](Project, Dataset, Table, DataFormat.AVRO, Some(TableReadOptions(List("col1"))))
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue shouldBe List.fill(DefaultNumStreams * ResponsesPerStream)(records)
    }

    "stream the results for a query using arrow deserializer" in {
      val reader = new SimpleRowReader(ArrowSchema(serializedSchema = GCPSerializedArrowSchema))
      val expectedRecords = reader.read(ArrowRecordBatch(GCPSerializedArrowTenRecordBatch, 10))

      implicit val um: ArrowByteStringDecoder = new ArrowByteStringDecoder(ArrowSchema(GCPSerializedArrowSchema))

      val seq = BigQueryStorage
        .typed[List[BigQueryRecord]](Project, Dataset, Table, DataFormat.ARROW, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      seq shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)(expectedRecords)
    }

    "fail if unable to connect to bigquery" in {
      val error = BigQueryStorage
        .create(Project, Dataset, Table, DataFormat.AVRO)
        .withAttributes(mockBQReader(port = 1234))
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.UNAVAILABLE
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "fail if the project is incorrect" in {
      val error = BigQueryStorage
        .create("NOT A PROJECT", Dataset, Table, DataFormat.AVRO)
        .withAttributes(mockBQReader())
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "fail if the dataset is incorrect" in {
      val error = BigQueryStorage
        .create(Project, "NOT A DATASET", Table, DataFormat.AVRO)
        .withAttributes(mockBQReader())
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "fail if the table is incorrect" in {
      val error = BigQueryStorage
        .create(Project, Dataset, "NOT A TABLE", DataFormat.AVRO)
        .withAttributes(mockBQReader())
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "stream the results for a query" in {
      val seq = BigQueryStorage
        .create(Project, Dataset, Table, DataFormat.AVRO, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      seq(0)._1 shouldBe storageAvroSchema
    }

  }

  def mockBQReader(host: String = bqHost, port: Int = bqPort) = {
    val reader = GrpcBigQueryStorageReader(BigQueryStorageSettings(host, port))
    BigQueryStorageAttributes.reader(reader)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    startMock()
  }

  override def afterAll(): Unit = {
    stopMock()
    system.terminate()
    super.afterAll()
  }

}
