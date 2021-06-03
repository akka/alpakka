/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.stream.alpakka.googlecloud.bigquery.storage.impl.SimpleRowReader
import akka.stream.alpakka.googlecloud.bigquery.storage.mock.ArrowRecords.{GCPSerializedSchema, GCPSerializedTenRecordBatch}
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryStorageSettings, BigQueryStorageSpecBase}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Sink
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BigQueryArrowStorageSpec
  extends BigQueryStorageSpecBase(21002)
  with AnyWordSpecLike
  with BeforeAndAfterAll
  with Matchers
  with LogCapturing {

  "BigQueryArrowStorage.readArrow" should {

    val reader = new SimpleRowReader(ArrowSchema(serializedSchema = GCPSerializedSchema))
    val expectedRecords = reader.read(ArrowRecordBatch(GCPSerializedTenRecordBatch,10))

    "stream the results for a query merged" in {
      val expectedSchema = ArrowSchema(serializedSchema = GCPSerializedSchema)
      BigQueryArrowStorage
        .readMerged(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .map(s => s._2.map(b => (s._1, b)))
        .flatMapMerge(100, identity)
        .runWith(Sink.seq)
        .futureValue shouldBe Vector.fill(DefaultNumStreams * ResponsesPerStream)((ArrowSchema(serializedSchema = GCPSerializedSchema), ArrowRecordBatch(GCPSerializedTenRecordBatch,10)))
    }

    "stream the results for a query" in {
      val streamRes = BigQueryArrowStorage
        .read(Project, Dataset, Table, None)
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue.head

      val schema = streamRes._1

      val sd = MessageSerializer.deserializeSchema(
        new ReadChannel(
          new ByteArrayReadableSeekableByteChannel(
            schema.serializedSchema.toByteArray
          )
        )
      )

      sd shouldBe FullArrowSchema

      val recordBatch = streamRes._2
        .reduce( (a,b) => a.merge(b))
        .withAttributes(mockBQReader())
        .runWith(Sink.seq)
        .futureValue.head

      val rowReader = new SimpleRowReader(schema)
      val records = rowReader.read(recordBatch)

      records shouldBe expectedRecords
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
