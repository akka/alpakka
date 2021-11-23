/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.SystemMaterializer;
// #import
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.csv.javadsl.CsvToMap;

// #import
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.*;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CsvToMapTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  public void documentation() {
    // #flow-type
    // keep values as ByteString
    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow1 = CsvToMap.toMap();

    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow2 =
        CsvToMap.toMap(StandardCharsets.UTF_8);

    Flow<Collection<ByteString>, Map<String, ByteString>, ?> flow3 =
        CsvToMap.withHeaders("column1", "column2", "column3");

    // values as String (decode ByteString)
    Flow<Collection<ByteString>, Map<String, String>, ?> flow4 =
        CsvToMap.toMapAsStrings(StandardCharsets.UTF_8);

    Flow<Collection<ByteString>, Map<String, String>, ?> flow5 =
        CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "column1", "column2", "column3");
    // #flow-type
  }

  @Test
  public void parsedLineShouldBecomeMapKeys() throws Exception {
    CompletionStage<Map<String, ByteString>> completionStage =
        // #header-line
        // values as ByteString
        Source.single(ByteString.fromString("eins,zwei,drei\n1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.toMap(StandardCharsets.UTF_8))
            .runWith(Sink.head(), system);
    // #header-line
    Map<String, ByteString> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #header-line

    assertThat(map.get("eins"), equalTo(ByteString.fromString("1")));
    assertThat(map.get("zwei"), equalTo(ByteString.fromString("2")));
    assertThat(map.get("drei"), equalTo(ByteString.fromString("3")));
    // #header-line
  }

  @Test
  public void parsedLineShouldBecomeMapKeysAndStringValues() throws Exception {
    CompletionStage<Map<String, String>> completionStage =
        // #header-line

        // values as String
        Source.single(ByteString.fromString("eins,zwei,drei\n1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
            .runWith(Sink.head(), system);
    // #header-line
    Map<String, String> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #header-line

    assertThat(map.get("eins"), equalTo("1"));
    assertThat(map.get("zwei"), equalTo("2"));
    assertThat(map.get("drei"), equalTo("3"));
    // #header-line
  }

  @Test
  public void givenHeadersShouldBecomeMapKeys() throws Exception {
    CompletionStage<Map<String, ByteString>> completionStage =
        // #column-names
        // values as ByteString
        Source.single(ByteString.fromString("1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.withHeaders("eins", "zwei", "drei"))
            .runWith(Sink.head(), system);
    // #column-names
    Map<String, ByteString> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #column-names

    assertThat(map.get("eins"), equalTo(ByteString.fromString("1")));
    assertThat(map.get("zwei"), equalTo(ByteString.fromString("2")));
    assertThat(map.get("drei"), equalTo(ByteString.fromString("3")));
    // #column-names
  }

  @Test
  public void givenHeadersShouldBecomeMapKeysAndStringValues() throws Exception {
    CompletionStage<Map<String, String>> completionStage =
        // #column-names

        // values as String
        Source.single(ByteString.fromString("1,2,3"))
            .via(CsvParsing.lineScanner())
            .via(CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "eins", "zwei", "drei"))
            .runWith(Sink.head(), system);
    // #column-names
    Map<String, String> map = completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #column-names

    assertThat(map.get("eins"), equalTo("1"));
    assertThat(map.get("zwei"), equalTo("2"));
    assertThat(map.get("drei"), equalTo("3"));
    // #column-names
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() throws Exception {
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(SystemMaterializer.get(system).materializer());
  }
}
