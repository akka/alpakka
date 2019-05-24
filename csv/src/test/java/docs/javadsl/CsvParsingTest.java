/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.csv.MalformedCsvException;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// #import
import akka.stream.alpakka.csv.javadsl.CsvParsing;

// #import
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    Flow<ByteString, Collection<ByteString>, NotUsed> flow =
        CsvParsing.lineScanner(delimiter, quoteChar, escapeChar);
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
  public void lineParserShouldParseOneLineAsString() throws Exception {
    CompletionStage<List<String>> completionStage =
        // #line-scanner-string
        Source.single(ByteString.fromString("eins,zwei,drei\n"))
            .via(CsvParsing.lineScanner())
            .map(line -> line.stream().map(ByteString::utf8String).collect(Collectors.toList()))
            .runWith(Sink.head(), materializer);
    // #line-scanner-string
    List<String> res = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(res.get(0), equalTo("eins"));
    assertThat(res.get(1), equalTo("zwei"));
    assertThat(res.get(2), equalTo("drei"));
  }

  @Test
  public void illegalFormatShouldThrow() throws Exception {
    CompletionStage<List<Collection<ByteString>>> completionStage =
        Source.single(ByteString.fromString("eins,zwei,drei\na,b,\\\"c"))
            .via(CsvParsing.lineScanner())
            .runWith(Sink.seq(), materializer);
    try {
      completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
      fail("Should throw MalformedCsvException");
    } catch (ExecutionException expected) {
      Throwable cause = expected.getCause();
      assertThat(cause, is(instanceOf(MalformedCsvException.class)));
      MalformedCsvException csvException = (MalformedCsvException) cause;
      assertThat(csvException.getLineNo(), equalTo(2L));
      assertThat(csvException.getBytePos(), equalTo(5));
    }
  }

  @Test
  public void escapeWithoutEscapedByteShouldProduceEscape() throws Exception {
    CompletionStage<List<String>> completionStage =
        Source.single(ByteString.fromString("a,b,\\c"))
            .via(CsvParsing.lineScanner())
            .map(line -> line.stream().map(ByteString::utf8String).collect(Collectors.toList()))
            .runWith(Sink.head(), materializer);
    List<String> res = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(res.get(0), equalTo("a"));
    assertThat(res.get(1), equalTo("b"));
    assertThat(res.get(2), equalTo("\\c"));
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

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }
}
