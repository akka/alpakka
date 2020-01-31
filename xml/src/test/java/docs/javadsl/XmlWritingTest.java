/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.xml.*;
import akka.stream.alpakka.xml.javadsl.XmlWriting;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.xml.stream.XMLOutputFactory;

import static org.junit.Assert.assertEquals;

public class XmlWritingTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer materializer;

  @Test
  public void xmlWriter() throws InterruptedException, ExecutionException, TimeoutException {

    // #writer
    final Sink<ParseEvent, CompletionStage<String>> write =
        Flow.of(ParseEvent.class)
            .via(XmlWriting.writer())
            .map(ByteString::utf8String)
            .toMat(Sink.fold("", (acc, el) -> acc + el), Keep.right());
    // #writer

    final String doc =
        "<?xml version='1.0' encoding='UTF-8'?><doc><elem>elem1</elem><elem>elem2</elem></doc>";
    final List<ParseEvent> docList = new ArrayList<>();
    docList.add(StartDocument.getInstance());
    docList.add(StartElement.create("doc", Collections.emptyMap()));
    docList.add(StartElement.create("elem", Collections.emptyMap()));
    docList.add(Characters.create("elem1"));
    docList.add(EndElement.create("elem"));
    docList.add(StartElement.create("elem", Collections.emptyMap()));
    docList.add(Characters.create("elem2"));
    docList.add(EndElement.create("elem"));
    docList.add(EndElement.create("doc"));
    docList.add(EndDocument.getInstance());

    final CompletionStage<String> resultStage = Source.from(docList).runWith(write, materializer);

    resultStage
        .thenAccept((str) -> assertEquals(doc, str))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void xmlWriterNamespace()
      throws InterruptedException, ExecutionException, TimeoutException {

    // #writer
    final Sink<ParseEvent, CompletionStage<String>> write =
        Flow.of(ParseEvent.class)
            .via(XmlWriting.writer())
            .map(ByteString::utf8String)
            .toMat(Sink.fold("", (acc, el) -> acc + el), Keep.right());
    // #writer

    // #writer-usage
    final String doc =
        "<?xml version='1.0' encoding='UTF-8'?>"
            + "<bk:book xmlns:bk=\"urn:loc.gov:books\" xmlns:isbn=\"urn:ISBN:0-395-36341-6\">"
            + "<bk:title>Cheaper by the Dozen</bk:title><isbn:number>1568491379</isbn:number></bk:book>";
    final List<Namespace> nmList = new ArrayList<>();
    nmList.add(Namespace.create("urn:loc.gov:books", Optional.of("bk")));
    nmList.add(Namespace.create("urn:ISBN:0-395-36341-6", Optional.of("isbn")));
    final List<ParseEvent> docList = new ArrayList<>();
    docList.add(StartDocument.getInstance());
    docList.add(
        StartElement.create(
            "book",
            Collections.emptyList(),
            Optional.of("bk"),
            Optional.of("urn:loc.gov:books"),
            nmList));
    docList.add(
        StartElement.create(
            "title", Collections.emptyList(), Optional.of("bk"), Optional.of("urn:loc.gov:books")));
    docList.add(Characters.create("Cheaper by the Dozen"));
    docList.add(EndElement.create("title"));
    docList.add(
        StartElement.create(
            "number",
            Collections.emptyList(),
            Optional.of("isbn"),
            Optional.of("urn:ISBN:0-395-36341-6")));
    docList.add(Characters.create("1568491379"));
    docList.add(EndElement.create("number"));
    docList.add(EndElement.create("book"));
    docList.add(EndDocument.getInstance());

    final CompletionStage<String> resultStage = Source.from(docList).runWith(write, materializer);
    // #writer-usage

    resultStage
        .thenAccept((str) -> assertEquals(doc, str))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void xmlWriterProvidedFactory()
      throws InterruptedException, ExecutionException, TimeoutException {

    final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
    // #writer
    final Sink<ParseEvent, CompletionStage<String>> write =
        Flow.of(ParseEvent.class)
            .via(XmlWriting.writer(xmlOutputFactory))
            .map(ByteString::utf8String)
            .toMat(Sink.fold("", (acc, el) -> acc + el), Keep.right());
    // #writer

    final String doc =
        "<?xml version='1.0' encoding='UTF-8'?><doc><elem>elem1</elem><elem>elem2</elem></doc>";
    final List<ParseEvent> docList = new ArrayList<>();
    docList.add(StartDocument.getInstance());
    docList.add(StartElement.create("doc", Collections.emptyMap()));
    docList.add(StartElement.create("elem", Collections.emptyMap()));
    docList.add(Characters.create("elem1"));
    docList.add(EndElement.create("elem"));
    docList.add(StartElement.create("elem", Collections.emptyMap()));
    docList.add(Characters.create("elem2"));
    docList.add(EndElement.create("elem"));
    docList.add(EndElement.create("doc"));
    docList.add(EndDocument.getInstance());

    final CompletionStage<String> resultStage = Source.from(docList).runWith(write, materializer);

    resultStage
        .thenAccept((str) -> assertEquals(doc, str))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
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
