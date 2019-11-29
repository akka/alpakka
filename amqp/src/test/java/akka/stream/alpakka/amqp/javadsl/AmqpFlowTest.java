/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.amqp.AmqpLocalConnectionProvider;
import akka.stream.alpakka.amqp.AmqpWriteSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.WriteMessage;
import akka.stream.alpakka.amqp.WriteResult;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.util.ByteString;
import scala.collection.JavaConverters;

/** Needs a local running AMQP server on the default port with no password. */
public class AmqpFlowTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer materializer;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  private static AmqpWriteSettings settings() {
    final String queueName = "amqp-flow-spec" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    return AmqpWriteSettings.create(AmqpLocalConnectionProvider.getInstance())
        .withRoutingKey(queueName)
        .withDeclaration(queueDeclaration)
        .withBufferSize(10)
        .withConfirmationTimeout(Duration.ofMillis(200));
  }

  @Test
  public void shouldEmitConfirmationForPublishedMessagesInSimpleFlow() {
    shouldEmitConfirmationForPublishedMessages(AmqpFlow.create(settings()));
  }

  @Test
  public void shouldEmitConfirmationForPublishedMessagesInFlowWithConfirm() {
    shouldEmitConfirmationForPublishedMessages(AmqpFlow.createWithConfirm(settings()));
  }

  @Test
  public void shouldEmitConfirmationForPublishedMessagesInFlowWithConfirmUnordered() {
    shouldEmitConfirmationForPublishedMessages(AmqpFlow.createWithConfirmUnordered(settings()));
  }

  private void shouldEmitConfirmationForPublishedMessages(
      final Flow<WriteMessage, WriteResult, CompletionStage<Done>> flow) {

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final List<WriteResult> expectedOutput =
        input.stream().map(pt -> WriteResult.create(true)).collect(Collectors.toList());

    final TestSubscriber.Probe<WriteResult> result =
        Source.from(input)
            .map(s -> WriteMessage.create(ByteString.fromString(s)))
            .via(flow)
            .toMat(TestSink.probe(system), Keep.right())
            .run(materializer);

    result
        .request(input.size())
        .expectNextN(JavaConverters.asScalaBufferConverter(expectedOutput).asScala().toList());
  }

  @Test
  public void shouldPropagateContextInSimpleFlow() {
    shouldPropagateContext(AmqpFlowWithContext.create(settings()));
  }

  @Test
  public void shouldPropagateContextInFlowWithConfirm() {
    shouldPropagateContext(AmqpFlowWithContext.createWithConfirm(settings()));
  }

  private void shouldPropagateContext(
      FlowWithContext<WriteMessage, String, WriteResult, String, CompletionStage<Done>>
          flowWithContext) {

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final List<Pair<WriteResult, String>> expectedOutput =
        input.stream()
            .map(pt -> Pair.create(WriteResult.create(true), pt))
            .collect(Collectors.toList());

    final TestSubscriber.Probe<Pair<WriteResult, String>> result =
        Source.from(input)
            .asSourceWithContext(s -> s)
            .map(s -> WriteMessage.create(ByteString.fromString(s)))
            .via(flowWithContext)
            .asSource()
            .toMat(TestSink.probe(system), Keep.right())
            .run(materializer);

    result
        .request(input.size())
        .expectNextN(JavaConverters.asScalaBufferConverter(expectedOutput).asScala().toList());
  }

  @Test
  public void shouldPropagatePassThrough() {
    Flow<Pair<WriteMessage, String>, Pair<WriteResult, String>, CompletionStage<Done>> flow =
        AmqpFlow.createWithConfirmAndPassThroughUnordered(settings());

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final List<Pair<WriteResult, String>> expectedOutput =
        input.stream()
            .map(pt -> Pair.create(WriteResult.create(true), pt))
            .collect(Collectors.toList());

    final TestSubscriber.Probe<Pair<WriteResult, String>> result =
        Source.from(input)
            .map(s -> Pair.create(WriteMessage.create(ByteString.fromString(s)), s))
            .via(flow)
            .toMat(TestSink.probe(system), Keep.right())
            .run(materializer);

    result
        .request(input.size())
        .expectNextN(JavaConverters.asScalaBufferConverter(expectedOutput).asScala().toList());
  }
}
