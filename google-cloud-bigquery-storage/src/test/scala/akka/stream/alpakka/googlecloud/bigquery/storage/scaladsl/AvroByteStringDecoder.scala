/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord
import akka.util.ByteString
import org.apache.avro.Schema
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

class AvroByteStringDecoder(schema: Schema) extends FromByteStringUnmarshaller[List[BigQueryRecord]] {

  val datumReader = new GenericDatumReader[GenericRecord](schema)

  override def apply(value: ByteString)(implicit ec: ExecutionContext,
                                        materializer: Materializer): Future[List[BigQueryRecord]] = {

    val result = ListBuffer[BigQueryRecord]()

    val inputStream = new SeekableByteArrayInput(value.toByteBuffer.array())
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    while (!decoder.isEnd) {
      val item = datumReader.read(null, decoder)

      result += BigQueryRecord.fromAvro(item)
    }

    Future(result.toList)
  }
}
