/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl
import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.Directory
import scala.util.Random

trait AbstractAvroParquet {
  case class Document(id: String, body: String)

  val genDocument: Gen[Document] =
    Gen.oneOf(Seq(Document(id = Gen.alphaStr.sample.get, body = Gen.alphaLowerStr.sample.get)))
  val genDocuments: Gen[List[Document]] = Gen.listOfN(3, genDocument)

  val folder: String = "./" + Random.alphanumeric.take(8).mkString("")
  val file = folder + "/test.parquet"
  val filePath = new org.apache.hadoop.fs.Path(file)

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}"
  )

  val conf = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

  def parquetReader[T](file: String, conf: Configuration) =
    AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def docToRecord(document: Document): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", document.id)
      .set("body", document.body)
      .build()

  def fromParquet[T <: GenericRecord](file: String, configuration: Configuration): List[T] = {
    val reader = parquetReader(file, conf)
    var record: T = reader.read()
    var result: List[T] = List.empty[T]
    while (record != null) {
      result = result ::: record :: Nil
      record = reader.read()
    }
    result
  }
}
