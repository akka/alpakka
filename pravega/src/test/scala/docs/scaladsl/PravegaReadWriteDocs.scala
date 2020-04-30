/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.pravega.scaladsl.Pravega
import akka.stream.alpakka.pravega.{PravegaEvent, ReaderSettingsBuilder, WriterSettingsBuilder}
import akka.stream.scaladsl.{Sink, Source}
import io.pravega.client.stream.impl.UTF8StringSerializer

object PravegaReadWriteDocs {

  implicit val system = ActorSystem("PravegaDocs")
  implicit val mat: Materializer = ActorMaterializer()

  val serializer = new UTF8StringSerializer

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
}
