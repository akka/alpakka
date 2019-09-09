/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.util.ByteString;
import scala.collection.JavaConverters;

/** Needs a local running AMQP server on the default port with no password. */
@RunWith(Parameterized.class)
public class AmqpFlowTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private final Flow<WriteMessage<String>, WriteResult<String>, CompletionStage<Done>> flow;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  public AmqpFlowTest(Flow<WriteMessage<String>, WriteResult<String>, CompletionStage<Done>> flow) {
    this.flow = flow;
  }

  private static AmqpWriteSettings settings() {
    final String queueName = "amqp-flow-spec" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    return AmqpWriteSettings.create(AmqpLocalConnectionProvider.getInstance())
        .withRoutingKey(queueName)
        .withDeclaration(queueDeclaration);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {AmqpFlow.create(settings())},
          {AmqpFlow.createWithConfirm(settings(), Duration.ofMillis(200))},
          {AmqpFlow.createWithAsyncConfirm(settings(), 10, Duration.ofMillis(200))},
          {AmqpFlow.createWithAsyncUnorderedConfirm(settings(), 10, Duration.ofMillis(200))}
        });
  }

  @Test
  public void shouldEmitConfirmationForPublishedMessages() throws Exception {

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final List<WriteResult<String>> expectedOutput =
        input.stream().map(pt -> WriteResult.create(true, pt)).collect(Collectors.toList());

    final Pair<CompletionStage<Done>, TestSubscriber.Probe<WriteResult<String>>> result =
        Source.from(input)
            .map(s -> WriteMessage.create(ByteString.fromString(s)).withPassThrough(s))
            .viaMat(flow, Keep.right())
            .toMat(TestSink.probe(system), Keep.both())
            .run(materializer);

    result
        .second()
        .request(input.size())
        .expectNextN(JavaConverters.asScalaBufferConverter(expectedOutput).asScala().toList());

    result.first().toCompletableFuture().get(1, TimeUnit.SECONDS);
  }
}
