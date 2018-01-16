/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet.scaladsl

import java.util.UUID

import akka.Done
import akka.stream.alpakka.parquet.Parquet.ParquetSettings
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future
import scala.reflect.ClassTag

object ParquetSink {

  def apply[T](settings: ParquetSettings, fileName: String)(implicit mat: ActorMaterializer, classTag: ClassTag[T]) =
    new ParquetSink[T](settings).plainSink(fileName)

  def partitionSink[T, K](settings: ParquetSettings, filePrefix: String)(groupBy: T => K, partitionName: K => String)(
      implicit mat: Materializer,
      classTag: ClassTag[T]
  ) =
    new ParquetSink[T](settings).partitioningSink(filePrefix)(groupBy, partitionName)

}

class ParquetSink[T](val settings: ParquetSettings)(implicit classTag: ClassTag[T]) {

  private val schema = ReflectData.get().getSchema(classTag.runtimeClass)

  def plainSink(fileName: String)(implicit mat: ActorMaterializer): Sink[T, Future[Done]] = {
    import mat.executionContext

    val writer = createWriter(s"$fileName.parquet", schema, settings.writeMode, settings.compressionCodeName)

    Flow[T]
      .toMat(Sink.foreach(record => {
        writer.write(record)
      }))(Keep.right)
      .mapMaterializedValue(done => {
        done.andThen({ case _ => writer.close() })
      })
  }

  def partitioningSink[K](
      filePrefix: String
  )(groupBy: T => K, partitionName: K => String)(implicit mat: Materializer): Sink[T, Future[Done]] = {

    implicit val ec = mat.executionContext

    Flow[T]
      .groupedWithin(settings.maxRecordsPerWindow, settings.window)
      .mapConcat(_.groupBy(groupBy).toList)
      .toMat(Sink.foreachParallel[(K, Seq[T])](settings.writeParallelism)({
        case (partition, rs) =>
          val partitionPrefix = partitionName(partition)
          val fileName = s"$partitionPrefix/${filePrefix}_${UUID.randomUUID()}.parquet"
          val writer = createWriter(fileName, schema, settings.writeMode, settings.compressionCodeName)
          rs.foreach(r => writer.write(r))
          writer.close()
      }))(Keep.right)
  }

  private def createWriter(fileName: String, schema: Schema, mode: Mode, compressionCodecName: CompressionCodecName) = {

    val conf = new Configuration()
    conf.setBoolean("parquet.enable.summary-metadata", false)
    AvroParquetWriter
      .builder[T](new Path(s"${settings.baseUri}/$fileName"))
      .withSchema(schema)
      .withDataModel(ReflectData.get())
      .withWriteMode(mode)
      .withCompressionCodec(compressionCodecName)
      .withConf(conf)
      .build()
  }

}
