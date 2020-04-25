/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File

import akka.testkit.TestKit
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.reflect.io.Directory
import scala.util.Random

trait AbstractAvroParquet extends BeforeAndAfterAll {
  this: Suite with TestKit =>

  case class Document(id: String, body: String)

  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}"
  )

  val genDocument: Gen[Document] =
    Gen.oneOf(Seq(Document(id = Gen.alphaStr.sample.get, body = Gen.alphaLowerStr.sample.get)))
  val genDocuments: Int => Gen[List[Document]] = n => Gen.listOfN(n, genDocument)

  val format: RecordFormat[Document] = RecordFormat[Document]

  val avroSchema: Schema = AvroSchema[Document]

  val folder: String = "./" + Random.alphanumeric.take(8).mkString("")

  val genFinalFile: Gen[String] = for {
    fileName <- Gen.alphaLowerStr
  } yield { folder + "/" + fileName + ".parquet" }

  val genFile: Gen[String] = Gen.oneOf(Seq(Gen.alphaLowerStr.sample.get + ".parquet"))

  val conf: Configuration = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

  def avro4sWriter[T <: GenericRecord](file: String, conf: Configuration, schema: Schema): ParquetWriter[T] =
    AvroParquetWriter.builder[T](new Path(file)).withConf(conf).withSchema(schema).build()

  def parquetReader(file: String, conf: Configuration): ParquetReader[GenericRecord] =
    AvroParquetReader.builder[GenericRecord](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def parquetGReader[T <: GenericRecord](file: String, conf: Configuration): ParquetReader[T] =
    AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def docToRecord(document: Document): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", document.id)
      .set("body", document.body)
      .build()

  def fromParquet(file: String, configuration: Configuration): List[GenericRecord] = {
    val reader = parquetReader(file, conf)
    var record: GenericRecord = reader.read()
    var result: List[GenericRecord] = List.empty[GenericRecord]
    while (record != null) {
      result = result ::: record :: Nil
      record = reader.read()
    }
    result
  }

  def fromGenericParquet[T <: GenericRecord](file: String, configuration: Configuration): List[T] = {
    val reader = parquetGReader(file, conf)
    var record: T = reader.read()
    var result: List[T] = List.empty[T]
    while (record != null) {
      result = result ::: record :: Nil
      record = reader.read()
    }
    result
  }

  () =>
    new { //documentation
      // #prepare
      import org.apache.avro.generic.GenericRecord
      import org.apache.hadoop.conf.Configuration
      import org.apache.hadoop.fs.Path
      import org.apache.parquet.avro.AvroParquetReader
      import org.apache.parquet.hadoop.util.HadoopInputFile
      import org.apache.parquet.hadoop.ParquetReader
      import org.apache.parquet.avro.AvroReadSupport

      val file: String = "./sample/path/test.parquet"
      val conf: Configuration = new Configuration()
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
      // #prepare

      // #init-writer
      val writer: ParquetWriter[GenericRecord] =
        AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()
      // #init-writer
      // #init-reader
      val reader: ParquetReader[GenericRecord] =
        AvroParquetReader.builder[GenericRecord](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()
      // #init-reader
      if (writer != null && reader != null) { // forces val usage
      }
    }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }
}
