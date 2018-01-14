package akka.stream.alpakka.parquet

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.stream.alpakka.parquet.Parquet.ParquetSettings
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Parquet {

  case class ParquetSettings(baseUri: String, maxRecordsPerWindow: Int, window: FiniteDuration, writeMode: Mode = Mode.CREATE, compressionCodeName: CompressionCodecName = CompressionCodecName.SNAPPY, writeParallelism: Int = 1) {
    def withWriteMode(mode: Mode) = copy(writeMode = mode)

    def withCompressionCodecName(compressionCodec: CompressionCodecName) = copy(compressionCodeName = compressionCodec)

    def withWriteParallelism(parallelism: Int) = copy(writeParallelism = parallelism)
  }

  def apply[T](settings: ParquetSettings)(implicit system: ActorSystem, sf: SchemaFor[T], tr: ToRecord[T], fr: FromRecord[T]) = new Parquet[T](settings)

}


class Parquet[T](val settings: ParquetSettings)(implicit system: ActorSystem, sf: SchemaFor[T], tr: ToRecord[T], fr: FromRecord[T]) {

  private val schema = AvroSchema[T]
  private val format = RecordFormat[T]

  private val log = Logging(system, "parquet-sink")

  def plainSink(fileName: String): Sink[T, Future[Done]] =
    Flow[T].groupedWithin(settings.maxRecordsPerWindow, settings.window)
      .toMat(Sink.foreach(records => {

        val fileName = s"$fileName.parquet"
        val writer = createWriter(fileName, settings.writeMode, settings.compressionCodeName)

        records.foreach(r => writer.write(format.to(r)))
        writer.close()

      }))(Keep.right)

  def partitioningSink[K](prefix: String)(groupBy: T => K, partitionName: K => String)(implicit mat: ActorMaterializer): Sink[T, Future[Done]] = {

    implicit val ec = mat.executionContext

    Flow[T].groupedWithin(settings.maxRecordsPerWindow, settings.window)
      .mapConcat(_.groupBy(groupBy).toList)
      .toMat(Sink.foreachParallel[(K, Seq[T])](settings.writeParallelism)({
        case (partition, rs) =>
          log.debug(s"Processing partition $partition with ${rs.length} records")
          val partitionPrefix = partitionName(partition)
          val fileName = s"$partitionPrefix/${prefix}_${UUID.randomUUID()}.parquet"
          val writer = createWriter(fileName, settings.writeMode, settings.compressionCodeName)
          rs.foreach(r => writer.write(format.to(r)))
          writer.close()
      }))(Keep.right)
  }

  private def createWriter(fileName: String, mode: Mode, compressionCodecName: CompressionCodecName) =
    AvroParquetWriter.builder[GenericRecord](new Path(s"${settings.baseUri}/$fileName"))
      .withSchema(schema)
      .withWriteMode(mode)
      .withCompressionCodec(compressionCodecName)
      .build()

}

