/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.csv.MalformedCsvException;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// #line-scanner

// #line-scanner
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
    public void lineParserShouldParseOneLine() throws Exception {
        CompletionStage<Collection<ByteString>> completionStage =
        // #line-scanner
            Source.single(ByteString.fromString("eins,zwei,drei\n"))
                .via(CsvParsing.lineScanner())
                .runWith(Sink.head(), materializer);
        // #line-scanner
        Collection<ByteString> list = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
        String[] res = list.stream().map(ByteString::utf8String).toArray(String[]::new);
        assertThat(res[0], equalTo("eins"));
        assertThat(res[1], equalTo("zwei"));
        assertThat(res[2], equalTo("drei"));
    }

    @Test
    public void illegalFormatShouldThrow() throws Exception {
        CompletionStage<List<Collection<ByteString>>> completionStage =
            Source.single(ByteString.fromString("eins,zwei,drei\na,b,\\c"))
                    .via(CsvParsing.lineScanner())
                    .runWith(Sink.seq(), materializer);
        try {
            completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
             fail("Should throw MalformedCsvException");
        }
        catch (ExecutionException expected) {
            Throwable cause = expected.getCause();
            assertThat(cause, is(instanceOf(MalformedCsvException.class)));
            MalformedCsvException csvException = (MalformedCsvException) cause;
            assertThat(csvException.getLineNo(), equalTo(2L));
            assertThat(csvException.getBytePos(), equalTo(4));
        }
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
