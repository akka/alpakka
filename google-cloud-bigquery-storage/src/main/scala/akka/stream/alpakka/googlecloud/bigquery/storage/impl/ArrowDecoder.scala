/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.scaladsl.Flow
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient, ReadRowsRequest}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.{asJavaIterableConverter, collectionAsScalaIterableConverter}

object ArrowDecoder {

}


@InternalApi private[bigquery] class ArrowDecoder {

  private val RequestParamsHeader = "x-goog-request-params"

  def flow(client: BigQueryReadClient): Flow[ReadSession, List[BigQueryRecord], NotUsed] =
    Flow[ReadSession].map (
      rs => {
        val arrowSchema = rs.schema
        rs.streams.map((arrowSchema, _)).toList
      }
    )
      .splitWhen(_ => true)
      .mapConcat(a => a)
      .map(rs =>
        client.readRows()
          .addHeader(RequestParamsHeader, s"read_stream=${rs._2.name}")
          .invoke(ReadRowsRequest(rs._2.name))
          .mapConcat(_.rows.arrowRecordBatch.toList)
          .map((rs._1, _))

      )
      .mergeSubstreams
      .flatMapConcat(a => a)
      .map(a => SimpleRowReader(a._1.arrowSchema.get).read(a._2))


}

final object SimpleRowReader {

  def apply(schema: ArrowSchema): SimpleRowReader = {
    val allocator = new RootAllocator(Long.MaxValue)
    val schemaRoot = createVectorSchemaRoot(allocator, schema)
    val loader = vectorLoader(schemaRoot)
    new SimpleRowReader(schema, schemaRoot, loader)
  }

  def vectorLoader(schema: VectorSchemaRoot): VectorLoader = {
    new VectorLoader(schema)
  }

  def createVectorSchemaRoot(allocator: RootAllocator, schema: ArrowSchema): VectorSchemaRoot = {
    val sd = MessageSerializer.deserializeSchema(
      new ReadChannel(
        new ByteArrayReadableSeekableByteChannel(
          schema.serializedSchema.toByteArray
        )
      )
    )

    val vl = sd.getFields.asScala.map(f => f.createVector(allocator)).asJava
    new VectorSchemaRoot(vl)
  }

}

final class SimpleRowReader(val schema: ArrowSchema, val root: VectorSchemaRoot, val loader: VectorLoader) extends AutoCloseable {

  val allocator = new RootAllocator(Long.MaxValue)

  def read(batch: ArrowRecordBatch): List[BigQueryRecord]= {
    val deserializedBatch = MessageSerializer.deserializeRecordBatch(new ReadChannel(new ByteArrayReadableSeekableByteChannel(
      batch.serializedRecordBatch.toByteArray
    )), allocator);
    loader.load(deserializedBatch)
    deserializedBatch.close()

    val rs = root.getSchema.getFields
    val fvs = root.getFieldVectors.asScala

    val recordsList = ListBuffer()
    for(i <- 0 until root.getRowCount) {
      val bigQueryRecord = BigQueryRecord()
      for(fv <- fvs) {
        bigQueryRecord.put(rs.get(i).getName, fv.getObject(i))
      }
      recordsList += bigQueryRecord
    }

    root.clear();
    recordsList.toList
  }

  override def close(): Unit = {
    root.close();
    allocator.close();
  }

}

