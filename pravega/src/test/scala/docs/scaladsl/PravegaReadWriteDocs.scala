/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
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
import akka.stream.alpakka.pravega.TableReaderSettingsBuilder
import akka.stream.alpakka.pravega.scaladsl.PravegaTable
import akka.stream.alpakka.pravega.scaladsl.Pravega
import scala.util.Using
import io.pravega.client.tables.TableKey

class PravegaReadWriteDocs {

  implicit val system = ActorSystem("PravegaDocs")

  val serializer = new UTF8StringSerializer

  implicit def personSerialiser: Serializer[Person] = ???

  implicit val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  val readerSettings = ReaderSettingsBuilder(system)
    .withSerializer(serializer)

  val writerSettings = WriterSettingsBuilder(system)
    .withSerializer(serializer)

  val writerSettingsWithRoutingKey = WriterSettingsBuilder(system)
    .withKeyExtractor((str: String) => str.take(1))
    .withSerializer(serializer)

  // #writing
  Source(1 to 100)
    .map(i => s"event_$i")
    .runWith(Pravega.sink("an_existing_scope", "an_existing_streamName", writerSettings))

  Source(1 to 100)
    .map { i =>
      val routingKey = i % 10
      s"${routingKey}_event_$i"
    }
    .runWith(Pravega.sink("an_existing_scope", "an_existing_streamName", writerSettingsWithRoutingKey))

  // #writing

  def processMessage(message: String): Unit = ???

  // #reader-group

  Using(Pravega.readerGroupManager("an_existing_scope", readerSettings.clientConfig)) { readerGroupManager =>
    readerGroupManager.createReaderGroup("myGroup", "stream1", "stream2")
  }
  // #reader-group
    .foreach { readerGroup =>
      // #reading

      Pravega
        .source(readerGroup, readerSettings)
        .to(Sink.foreach { event: PravegaEvent[String] =>
          val message: String = event.message
          processMessage(message)
        })
        .run()

      // #reading

    }

  implicit val tablewriterSettings = TableWriterSettingsBuilder[Int, Person]()
    .withKeyExtractor(id => new TableKey(intSerializer.serialize(id)))
    .build()

  // #table-writing

  // Write through a flow
  Source(1 to 10)
    .map(id => (id, Person(id, s"name_$id")))
    .via(PravegaTable.writeFlow("an_existing_scope", "an_existing_tablename", tablewriterSettings))
    .runWith(Sink.ignore)

  // Write in a sink
  Source(1 to 10)
    .map(id => (id, Person(id, s"name_$id")))
    .runWith(PravegaTable.sink("an_existing_scope", "an_existing_tablename", tablewriterSettings))

  // #table-writing

  val clientConfig = ClientConfig.builder().build()

  val tableSettings = TableReaderSettingsBuilder[Int, Person]()
    .withKeyExtractor(id => new TableKey(intSerializer.serialize(id)))
    .build()

  // #table-reading

  val readingDone = PravegaTable
    .source("an_existing_scope", "an_existing_tablename", tableSettings)
    .to(Sink.foreach(println))
    .run()

  // #table-reading

}
