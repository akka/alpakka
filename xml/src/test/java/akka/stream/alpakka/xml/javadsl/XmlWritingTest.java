/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.xml.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.xml.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

public class XmlWritingTest {
  private static ActorSystem system;
  private static Materializer materializer;

  @Test
  public void xmlWriter() throws InterruptedException, ExecutionException, TimeoutException {

    // #writer
    final Sink<ParseEvent, CompletionStage<String>> write = Flow.of(ParseEvent.class)
      .via(XmlWriting.writer())
      .map((ByteString bs) -> bs.utf8String())
      .toMat(Sink.fold("", (acc, el) -> acc + el), Keep.right());
    // #writer

    // #writer-usage
    final String doc = "<?xml version='1.0' encoding='UTF-8'?><doc><elem>elem1</elem><elem>elem2</elem></doc>";
    final List<ParseEvent> docList= new ArrayList<ParseEvent>();
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
    // #writer-usage

    resultStage.thenAccept((str) -> {
      assertEquals(doc,str);
    }).toCompletableFuture().get(5, TimeUnit.SECONDS);
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
