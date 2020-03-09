/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl._
import akka.stream.scaladsl.Sink
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.matchers.should.Matchers

class GoogleBigQueryStorageSpec extends BigQueryStorageSpecBase with Matchers {

  "GoogleBigQuery.read" should {
    "stream the results for a query, deserializing into generic records" in new BQFixture {
      GoogleBigQueryStorage
        .read(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe List.fill(DefaultNumStreams * ResponsesPerStream * RecordsPerReadRowsResponse)(Record)
    }

    "filter results based on the TableReadOptions provided" in new BQFixture {
      GoogleBigQueryStorage
        .read(Project, Dataset, Table, Some(TableReadOptions(rowRestriction = "true = false")))
        .withAttributes(mockBQReader())
        .flatMapConcat(identity)
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "restrict the number of streams/Sources returned by the Storage API, if specified" in new BQFixture {
      val maxStreams = 5
      GoogleBigQueryStorage
        .read(Project, Dataset, Table, maxNumStreams = maxStreams)
        .withAttributes(mockBQReader())
        .runFold(0)((acc, _) => acc + 1)
        .futureValue shouldBe maxStreams
    }

    "fail if unable to connect to bigquery" in new BQFixture {
      val error = GoogleBigQueryStorage
        .read(Project, Dataset, Table, None)
        .withAttributes(mockBQReader(port = 1234))
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.UNAVAILABLE
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "fail if the project is incorrect" in new BQFixture {
      val error = GoogleBigQueryStorage
        .read("NOT A PROJECT", Dataset, Table, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.ignore)
        .failed
        .futureValue

      error match {
        case sre: StatusRuntimeException => sre.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
        case other => fail(s"Expected a StatusRuntimeException, got $other")
      }
    }

    "fail if the dataset or table is incorrect" in new BQFixture {
      val error = GoogleBigQueryStorage
        .read(Project, "NOT A DATASET", "NOT A TABLE", None)
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

  trait BQFixture {

    def mockBQReader(host: String = bqHost, port: Int = bqPort) = {
      val reader = GrpcBigQueryStorageReader(BigQueryStorageSettings(host, port))
      BigQueryStorageAttributes.reader(reader)
    }

  }
}
