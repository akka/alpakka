/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryStorageSettings, BigQueryStorageSpecBase}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import com.google.cloud.bigquery.storage.v1.stream.DataFormat
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
        .readMergedStreams(Project, Dataset, Table, DataFormat.AVRO, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      val avroSchema = storageAvroSchema
      val avroRows = storageAvroRows

      seq shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)((avroSchema, avroRows))
    }
  }

  "GoogleBigQuery.read" should {
    "stream the results for a query" in {
      val seq = BigQueryStorage
        .read(Project, Dataset, Table, DataFormat.AVRO, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue

      seq(0)._1 shouldBe storageAvroSchema
    }
  }

  "GoogleBigQuery.readAvroOnly" should {
    "stream the results for a query, deserializing into generic records" in {
      BigQueryStorage
        .readAvroOnly(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe List.fill(DefaultNumStreams * ResponsesPerStream * RecordsPerReadRowsResponse)(
        FullAvroRecord
      )
    }

    "filter results based on the row restriction configured" in {
      BigQueryStorage
        .readAvroOnly(Project, Dataset, Table, Some(TableReadOptions(rowRestriction = "true = false")))
        .withAttributes(mockBQReader())
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "apply configured column restrictions" in {
      BigQueryStorage
        .readAvroOnly(Project, Dataset, Table, Some(TableReadOptions(List("col1"))))
        .withAttributes(mockBQReader())
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe List.fill(DefaultNumStreams * ResponsesPerStream * RecordsPerReadRowsResponse)(
        Col1AvroRecord
      )
    }

    "restrict the number of streams/Sources returned by the Storage API, if specified" in {
      val maxStreams = 5
      BigQueryStorage
        .readAvroOnly(Project, Dataset, Table, maxNumStreams = maxStreams)
        .withAttributes(mockBQReader())
        .runFold(0)((acc, _) => acc + 1)
        .futureValue shouldBe maxStreams
    }

    "fail if unable to connect to bigquery" in {
      val error = BigQueryStorage
        .readAvroOnly(Project, Dataset, Table, None)
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
        .readAvroOnly("NOT A PROJECT", Dataset, Table, None)
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
        .readAvroOnly(Project, "NOT A DATASET", Table, None)
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
        .readAvroOnly(Project, Dataset, "NOT A TABLE", None)
        .withAttributes(mockBQReader())
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
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
