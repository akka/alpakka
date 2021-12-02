/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AvroDecoder
import akka.stream.alpakka.googlecloud.bigquery.storage.{
  BigQueryRecord,
  BigQueryStorageSettings,
  BigQueryStorageSpecBase
}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

class BigQueryAvroStorageSpec
    extends BigQueryStorageSpecBase(21002)
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {

  "BigQueryAvroStorage.readAvro" should {
    val avroSchema = storageAvroSchema.value
    val avroRows = storageAvroRows.value

    "stream the results for a query in records merged" in {
      val decoder = AvroDecoder(avroSchema.schema)
      val records = decoder.decodeRows(avroRows.serializedBinaryRows).map(gr => BigQueryRecord.fromAvro(gr))

      BigQueryAvroStorage
        .readRecordsMerged(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue shouldBe Seq.fill(DefaultNumStreams * ResponsesPerStream)(records)
    }

    "stream the results for a query in records" in {
      val decoder = AvroDecoder(avroSchema.schema)
      val records = decoder.decodeRows(avroRows.serializedBinaryRows).map(gr => BigQueryRecord.fromAvro(gr))

      BigQueryAvroStorage
        .readRecords(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .map(a => a.reduce((a, b) => a.merge(b)))
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe Seq.fill(DefaultNumStreams * ResponsesPerStream)(records).flatten
    }

    "stream the results for a query merged" in {
      BigQueryAvroStorage
        .readMerged(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .map(s => s._2.map(b => (s._1, b)))
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)((avroSchema, avroRows))
    }

    "stream the results for a query" in {
      BigQueryAvroStorage
        .read(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .map { s =>
          s._2
            .reduce((a, b) => a.merge(b))
            .map(b => (s._1, b))
        }
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)((avroSchema, avroRows))
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
