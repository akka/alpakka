/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.pravega.*;
import akka.stream.alpakka.pravega.javadsl.Pravega;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.Arrays;
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

    PravegaReaderGroup readerGroup = null;

    // #reading
    CompletionStage<Done> fut =
        Pravega.<String>source(readerGroup, readerSettings)
            .to(Sink.foreach(e -> processMessage(e.message())))
            .run(system);
    // #reading

  }

  private static void processMessage(String message) {
    LOGGER.info(message);
  }
}
