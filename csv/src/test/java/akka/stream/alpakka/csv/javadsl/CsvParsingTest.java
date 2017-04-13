/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// #line-scanner

// #line-scanner
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class CsvParsingTest {
    private static ActorSystem system;
    private static Materializer materializer;

    public void documentation() {
        byte delimiter = CsvParsing.COMMA;
        byte quoteChar = CsvParsing.DOUBLE_QUOTE;
        byte escapeChar = CsvParsing.BACKSLASH;
        // #flow-type
        Flow<ByteString, Collection<ByteString>, NotUsed> flow
                = CsvParsing.lineScanner(delimiter, quoteChar, escapeChar);
        // #flow-type
    }

    @Test
    public void lineParserShouldParseOneLine() {
        CompletionStage<Collection<ByteString>> completionStage =
        // #line-scanner
            Source.single(ByteString.fromString("eins,zwei,drei\n"))
                .via(CsvParsing.lineScanner())
                .runWith(Sink.head(), materializer);
        // #line-scanner
        completionStage.thenAccept((list) -> {
            String[] res = list.stream().map(ByteString::utf8String).toArray(String[]::new);
            assertThat(res[0], equalTo("eins"));
            assertThat(res[1], equalTo("zwei"));
            assertThat(res[2], equalTo("drei"));
        });

    }

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void teardown() throws Exception {
        JavaTestKit.shutdownActorSystem(system);
    }
}
