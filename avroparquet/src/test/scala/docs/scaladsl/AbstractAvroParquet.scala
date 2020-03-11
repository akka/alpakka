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
import org.apache.parquet.avro.AvroReadSupport

import scala.util.Random

trait AbstractAvroParquet {

  case class Document(id: String, body: String)

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-system

  val conf = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}"
  )

  val folder: String = "./" + Random.alphanumeric.take(8).mkString("")

  def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)

    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

  def docToRecord(document: Document): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", document.id)
      .set("body", document.body)
      .build()

}
