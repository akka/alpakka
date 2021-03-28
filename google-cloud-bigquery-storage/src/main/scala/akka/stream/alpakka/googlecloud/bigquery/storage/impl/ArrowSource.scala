/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.impl

import akka.NotUsed
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.arrow.{ArrowRecordBatch, ArrowSchema}
import com.google.cloud.bigquery.storage.v1.storage.BigQueryReadClient
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object ArrowSource {

  def readRecords(client: BigQueryReadClient, readSession: ReadSession): Source[List[BigQueryRecord], NotUsed] =
    SDKClientSource
      .read(client, readSession)
      .mapConcat(_.arrowRecordBatch.toList)
      .map(new SimpleRowReader(readSession.schema.arrowSchema.get).read(_))

  def read(client: BigQueryReadClient,
           readSession: ReadSession): Source[(ReadSession.Schema, ArrowRecordBatch), NotUsed] =
    SDKClientSource
      .read(client, readSession)
      .mapConcat(_.arrowRecordBatch.toList)
      .map((readSession.schema, _))

}

final class SimpleRowReader(val schema: ArrowSchema) extends AutoCloseable {

  val allocator = new RootAllocator(Long.MaxValue)

  val sd = MessageSerializer.deserializeSchema(
    new ReadChannel(
      new ByteArrayReadableSeekableByteChannel(
        schema.serializedSchema.toByteArray
      )
    )
  )

  val vec = sd.getFields.asScala.map(_.createVector(allocator))
  var root = new VectorSchemaRoot(vec.asJava)
  val loader = new VectorLoader(root)

  def read(batch: ArrowRecordBatch): List[BigQueryRecord] = {
    val deserializedBatch = MessageSerializer.deserializeRecordBatch(new ReadChannel(
                                                                       new ByteArrayReadableSeekableByteChannel(
                                                                         batch.serializedRecordBatch.toByteArray
                                                                       )
                                                                     ),
                                                                     allocator);
    loader.load(deserializedBatch)
    deserializedBatch.close()

    val rs = root.getSchema.getFields
    val fvs = root.getFieldVectors.asScala

    val recordsList = ListBuffer[BigQueryRecord]()
    for (i <- 0 until root.getRowCount) {
      val map = mutable.Map[String, Object]()
      for (fv <- fvs) {
        map.put(rs.get(i).getName, fv.getObject(i))
      }
      recordsList += BigQueryRecord.fromMap(map.toMap)
    }

    root.clear();
    recordsList.toList
  }

  override def close(): Unit = {
    root.close();
    allocator.close();
  }

}
