/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.xml.Characters;
import akka.stream.alpakka.xml.EndDocument;
import akka.stream.alpakka.xml.EndElement;
import akka.stream.alpakka.xml.ParseEvent;
import akka.stream.alpakka.xml.StartDocument;
import akka.stream.alpakka.xml.StartElement;
import akka.stream.alpakka.xml.TextEvent;
import akka.stream.alpakka.xml.javadsl.XmlParsing;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.w3c.dom.Element;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class XmlParsingTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  @Test
  public void xmlParser() throws InterruptedException, ExecutionException, TimeoutException {

    // #parser
    final Sink<String, CompletionStage<List<ParseEvent>>> parse =
        Flow.<String>create()
            .map(ByteString::fromString)
            .via(XmlParsing.parser())
            .toMat(Sink.seq(), Keep.right());
    // #parser

    // #parser-usage
    final String doc = "<doc><elem>elem1</elem><elem>elem2</elem></doc>";
    final CompletionStage<List<ParseEvent>> resultStage = Source.single(doc).runWith(parse, system);
    // #parser-usage

    resultStage
        .thenAccept(
            (list) -> {
              assertThat(
                  list,
                  hasItems(
                      StartDocument.getInstance(),
                      StartElement.create("doc", Collections.emptyMap()),
                      StartElement.create("elem", Collections.emptyMap()),
                      Characters.create("elem1"),
                      EndElement.create("elem"),
                      StartElement.create("elem", Collections.emptyMap()),
                      Characters.create("elem2"),
                      EndElement.create("elem"),
                      EndElement.create("doc"),
                      EndDocument.getInstance()));
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void parseAndReadEvents() throws Exception {
    // #parser-to-data
    ByteString doc = ByteString.fromString("<doc><elem>elem1</elem><elem>elem2</elem></doc>");
    CompletionStage<List<String>> stage =
        Source.single(doc)
            .via(XmlParsing.parser())
            .statefulMapConcat(
                () -> {
                  // state
                  final StringBuilder textBuffer = new StringBuilder();
                  // aggregation function
                  return parseEvent -> {
                    switch (parseEvent.marker()) {
                      case XMLStartElement:
                        textBuffer.delete(0, textBuffer.length());
                        return Collections.emptyList();
                      case XMLEndElement:
                        EndElement s = (EndElement) parseEvent;
                        switch (s.localName()) {
                          case "elem":
                            String text = textBuffer.toString();
                            return Collections.singleton(text);
                          default:
                            return Collections.emptyList();
                        }
                      case XMLCharacters:
                      case XMLCData:
                        TextEvent t = (TextEvent) parseEvent;
                        textBuffer.append(t.text());
                        return Collections.emptyList();
                      default:
                        return Collections.emptyList();
                    }
                  };
                })
            .runWith(Sink.seq(), system);

    List<String> list = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(list, hasItems("elem1", "elem2"));
    // #parser-to-data
  }

  @Test
  public void xmlParserConfigured()
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean[] configWasCalled = {false};
    Consumer<AsyncXMLInputFactory> configureFactory = factory -> configWasCalled[0] = true;
    final Sink<String, CompletionStage<List<ParseEvent>>> parse =
        Flow.<String>create()
            .map(ByteString::fromString)
            .via(XmlParsing.parser(configureFactory))
            .toMat(Sink.seq(), Keep.right());

    final String doc = "<doc><elem>elem1</elem><elem>elem2</elem></doc>";
    final CompletionStage<List<ParseEvent>> resultStage = Source.single(doc).runWith(parse, system);

    resultStage
        .thenAccept(
            (list) -> {
              assertThat(
                  list,
                  hasItems(
                      StartDocument.getInstance(),
                      StartElement.create("doc", Collections.emptyMap()),
                      StartElement.create("elem", Collections.emptyMap()),
                      Characters.create("elem1"),
                      EndElement.create("elem"),
                      StartElement.create("elem", Collections.emptyMap()),
                      Characters.create("elem2"),
                      EndElement.create("elem"),
                      EndElement.create("doc"),
                      EndDocument.getInstance()));
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertThat(configWasCalled[0], is(true));
  }

  @Test
  public void xmlSubslice() throws InterruptedException, ExecutionException, TimeoutException {

    // #subslice
    final Sink<String, CompletionStage<List<ParseEvent>>> parse =
        Flow.<String>create()
            .map(ByteString::fromString)
            .via(XmlParsing.parser())
            .via(XmlParsing.subslice(Arrays.asList("doc", "elem", "item")))
            .toMat(Sink.seq(), Keep.right());
    // #subslice

    // #subslice-usage
    final String doc =
        "<doc>"
            + "  <elem>"
            + "    <item>i1</item>"
            + "    <item><sub>i2</sub></item>"
            + "    <item>i3</item>"
            + "  </elem>"
            + "</doc>";
    final CompletionStage<List<ParseEvent>> resultStage = Source.single(doc).runWith(parse, system);
    // #subslice-usage

    resultStage
        .thenAccept(
            (list) -> {
              assertThat(
                  list,
                  hasItems(
                      Characters.create("i1"),
                      StartElement.create("sub", Collections.emptyMap()),
                      Characters.create("i2"),
                      EndElement.create("sub"),
                      Characters.create("i3")));
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void xmlSubtree() throws InterruptedException, ExecutionException, TimeoutException {

    // #subtree
    final Sink<String, CompletionStage<List<Element>>> parse =
        Flow.<String>create()
            .map(ByteString::fromString)
            .via(XmlParsing.parser())
            .via(XmlParsing.subtree(Arrays.asList("doc", "elem", "item")))
            .toMat(Sink.seq(), Keep.right());
    // #subtree

    // #subtree-usage
    final String doc =
        "<doc>"
            + "  <elem>"
            + "    <item>i1</item>"
            + "    <item><sub>i2</sub></item>"
            + "    <item>i3</item>"
            + "  </elem>"
            + "</doc>";
    final CompletionStage<List<Element>> resultStage = Source.single(doc).runWith(parse, system);
    // #subtree-usage

    resultStage
        .thenAccept(
            (list) -> {
              assertThat(
                  list.stream().map(e -> XmlHelper.asString(e).trim()).collect(Collectors.toList()),
                  hasItems("<item>i1</item>", "<item><sub>i2</sub></item>", "<item>i3</item>"));
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }
}
