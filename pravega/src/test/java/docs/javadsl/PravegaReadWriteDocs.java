/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.japi.Pair;
import akka.stream.alpakka.pravega.*;
import akka.stream.alpakka.pravega.javadsl.Pravega;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTableClientConfiguration;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class PravegaReadWriteDocs extends PravegaAkkaTestCaseSupport {

  public static void docs() {
    WriterSettings<String> writerSettings =
        WriterSettingsBuilder.<String>create(system).withSerializer(new JavaSerializer<>());
    WriterSettings<String> writerSettingsWithRoutingKey =
        WriterSettingsBuilder.<String>create(system)
            .withKeyExtractor((String str) -> str.substring(0, 2))
            .withSerializer(new JavaSerializer<>());
    ReaderSettings<String> readerSettings =
        ReaderSettingsBuilder.create(system).withSerializer(new JavaSerializer<>());

    // #writing
    Sink<String, CompletionStage<Done>> sinkWithRouting =
        Pravega.sink("an_existing_scope", "an_existing_scope", writerSettings);

    CompletionStage<Done> doneWithRouting =
        Source.from(Arrays.asList("One", "Two", "Three")).runWith(sinkWithRouting, system);

    // #writing

    // #reading
    CompletionStage<Done> fut =
        Pravega.<String>source("an_existing_scope", "an_existing_scope", readerSettings)
            .to(Sink.foreach(e -> processMessage(e.message())))
            .run(system);
    // #reading

    UTF8StringSerializer serializer = new UTF8StringSerializer();

    Serializer<Integer> intSerializer =
        new Serializer<Integer>() {
          public ByteBuffer serialize(Integer value) {
            ByteBuffer buff = ByteBuffer.allocate(4).putInt(value);
            buff.position(0);
            return buff;
          }

          public Integer deserialize(ByteBuffer serializedValue) {

            return serializedValue.getInt();
          }
        };

    TableWriterSettings<Integer, String> tablewriterSettings =
        TableWriterSettingsBuilder.<Integer, String>create(system)
            .withSerializers(intSerializer, serializer);

    // #table-writing
    final List<Person> events =
        Arrays.asList(
            new Person(1, "One"),
            new Person(2, "Two"),
            new Person(3, "Three"),
            new Person(4, "Four"));

    Sink<Person, CompletionStage<Done>> sink =
        Pravega.tableSink(
            "an_existing_scope",
            "an_existing_tableName",
            tablewriterSettings,
            (Person p) -> new scala.Tuple2<>(p.id(), p.firstname()));

    CompletionStage<Done> done = Source.from(events).toMat(sink, Keep.right()).run(system);

    // #table-writing

    TableSettings<Integer, String> tableSettings =
        TableSettingsBuilder.<Integer, String>create(system)
            .withKVSerializers(intSerializer, serializer);

    // #table-reading

    final CompletionStage<Done> pair =
        Pravega.tableSource("an_existing_scope", "an_existing_tableName", "test", tableSettings)
            .to(Sink.foreach((Pair<Integer, String> kvp) -> processKVP(kvp)))
            .run(system);
    // #table-reading

  }

  private static void processKVP(Pair<Integer, String> kvp) {
    LOGGER.info(kvp.toString());
  }

  private static void processMessage(String message) {
    LOGGER.info(message);
  }
}
