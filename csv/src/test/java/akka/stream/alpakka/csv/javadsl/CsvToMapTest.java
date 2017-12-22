/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
// #header-line

// #header-line

// #column-names

// #column-names

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class CsvToMapTest {
    private static ActorSystem system;
    private static Materializer materializer;

    public void documentation() {
        // #flow-type
        Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow1
                = CsvToMap.toMap();

        Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow2
                = CsvToMap.toMap(StandardCharsets.UTF_8);

        Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow3
                = CsvToMap.withHeaders("column1", "column2", "column3");
        // #flow-type
    }

    @Test
    public void parsedLineShouldBecomeMapKeys() {
        CompletionStage<Map<String, ByteString>> completionStage =
        // #header-line
            Source
                .single(ByteString.fromString("eins,zwei,drei\n1,2,3"))
                .via(CsvParsing.lineScanner())
                .via(CsvToMap.toMap(StandardCharsets.UTF_8))
                .runWith(Sink.head(), materializer);
        // #header-line
        completionStage.thenAccept((map) -> {
            assertThat(map.get("eins"), equalTo("1"));
            assertThat(map.get("zwei"), equalTo("2"));
            assertThat(map.get("drei"), equalTo("3"));
        });

    }

    @Test
    public void givenHeadersShouldBecomeMapKeys() {
        CompletionStage<Map<String, ByteString>> completionStage =
        // #column-names
            Source
                .single(ByteString.fromString("1,2,3"))
                .via(CsvParsing.lineScanner())
                .via(CsvToMap.withHeaders("eins", "zwei", "drei"))
                .runWith(Sink.head(), materializer);
        // #column-names
        completionStage.thenAccept((map) -> {
            assertThat(map.get("eins"), equalTo("1"));
            assertThat(map.get("zwei"), equalTo("2"));
            assertThat(map.get("drei"), equalTo("3"));
        });

    }

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void teardown() throws Exception {
        TestKit.shutdownActorSystem(system);
    }
}
