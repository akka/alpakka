/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryStorageSettings, BigQueryStorageSpecBase}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import com.google.cloud.bigquery.storage.v1.storage.ReadRowsResponse.Rows.AvroRows
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.Schema.AvroSchema
import org.scalatest.matchers.should.Matchers

class BigQueryAvroStorageSpec
    extends BigQueryStorageSpecBase(21002)
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {

  "BigQueryAvroStorage.readAvro" should {
    "stream the results for a query" in {
      val seq = BigQueryAvroStorage
        .read(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .map {
          s => s._2.reduce((a, b) => a.merge(b))
            .map(b => (s._1, b))
        }
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue

      val avroSchema = storageAvroSchema.value
      val avroRows =  storageAvroRows.value

      seq shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream )((avroSchema, avroRows))
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
