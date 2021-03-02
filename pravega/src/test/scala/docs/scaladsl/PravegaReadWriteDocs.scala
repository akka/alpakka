/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.pravega.scaladsl.Pravega
import akka.stream.alpakka.pravega.{
  PravegaEvent,
  ReaderSettingsBuilder,
  TableWriterSettingsBuilder,
  WriterSettingsBuilder
}
import akka.stream.scaladsl.{Sink, Source}
import io.pravega.client.ClientConfig
import io.pravega.client.stream.Serializer
import io.pravega.client.stream.impl.UTF8StringSerializer

import java.nio.ByteBuffer
import akka.stream.alpakka.pravega.TableSettingsBuilder
import akka.stream.alpakka.pravega.scaladsl.PravegaTable
import akka.stream.alpakka.pravega.scaladsl.Pravega

class PravegaReadWriteDocs {

  implicit val system = ActorSystem("PravegaDocs")

  val serializer = new UTF8StringSerializer

  val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  implicit val readerSettings = ReaderSettingsBuilder(system)
    .withSerializer(serializer)

  implicit val writerSettings = WriterSettingsBuilder(system)
    .withSerializer(serializer)

  val writerSettingsWithRoutingKey = WriterSettingsBuilder(system)
    .withKeyExtractor((str: String) => str.take(1))
    .withSerializer(serializer)

  // #writing
  Source(1 to 100).map(i => s"event_$i").runWith(Pravega.sink("an_existing_scope", "an_existing_streamName"))

  Source(1 to 100)
    .map { i =>
      val routingKey = i % 10
      s"${routingKey}_event_$i"
    }
    .runWith(Pravega.sink("an_existing_scope", "an_existing_streamName")(writerSettingsWithRoutingKey))

  // #writing

  def processMessage(message: String): Unit = ???

  // #reading

  val fut = Pravega
    .source("an_existing_scope", "an_existing_streamName")
    .to(Sink.foreach { event: PravegaEvent[String] =>
      val message: String = event.message
      processMessage(message)
    })
    .run()

  // #reading
  case class Person(id: String, firstname: String)

  implicit val tablewriterSettings = TableWriterSettingsBuilder[String, String](system)
    .withSerializers(serializer, serializer)

  // #table-writing

  Source(1 to 10)
    .map(id => Person(id = s"id_$id", firstname = s"name_$id"))
    .via(PravegaTable.writeFlow("an_existing_scope", "an_existing_tablename", (p: Person) => (p.id, p.firstname)))
    .runWith(Sink.ignore)

  Source(1 to 10)
    .map(id => Person(id = s"id_$id", firstname = s"name_$id"))
    .runWith(PravegaTable.sink("an_existing_scope", "an_existing_tablename", (p: Person) => (p.id, p.firstname)))

  // #table-writing

  val clientConfig = ClientConfig.builder().build()

  val tableSettings = TableSettingsBuilder
    .apply[Int, String](system.settings.config.getConfig(TableSettingsBuilder.configPath))
    //      .clientConfigBuilder(                                 KeyValueTableClientConfiguration.builder().build())
    .withKVSerializers(intSerializer, serializer)

  // #table-reading

  val readingDone = PravegaTable
    .source("an_existing_scope", "an_existing_tablename", "test", tableSettings)
    .to(Sink.foreach(println))
    .run()

  // #table-reading

}
