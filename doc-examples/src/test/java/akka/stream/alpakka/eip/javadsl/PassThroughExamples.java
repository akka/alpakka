/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.eip.javadsl;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function2;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.scaladsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.stream.UniformFanOutShape;
import akka.stream.FanInShape2;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.ZipWith;

public class PassThroughExamples {
  private static ActorSystem system;
  private static Materializer materializer;

  @Test
  public void passThroughWithKeep() throws InterruptedException, ExecutionException {
    // #PassThroughWithKeep
    // Sample Source
    Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3));

    // Pass through this flow maintaining the original message
    Flow<Integer, Integer, NotUsed> passThroughMe = Flow.of(Integer.class).map(i -> i * 10);

    CompletionStage<List<Integer>> ret =
        source
            .via(PassThroughFlow.create(passThroughMe, Keep.right()))
            .runWith(Sink.seq(), materializer);

    // Verify results
    List<Integer> list = ret.toCompletableFuture().get();
    assert list.equals(Arrays.asList(1, 2, 3));
    // #PassThroughWithKeep
  }

  @Test
  public void passThroughTuple() throws InterruptedException, ExecutionException {
    // #PassThroughTuple
    // Sample Source
    Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3));

    // Pass through this flow maintaining the original message
    Flow<Integer, Integer, NotUsed> passThroughMe = Flow.of(Integer.class).map(i -> i * 10);

    CompletionStage<List<Pair<Integer, Integer>>> ret =
        source.via(PassThroughFlow.create(passThroughMe)).runWith(Sink.seq(), materializer);

    // Verify results
    List<Pair<Integer, Integer>> list = ret.toCompletableFuture().get();
    assert list.equals(
        Arrays.asList(
            new Pair<Integer, Integer>(10, 1),
            new Pair<Integer, Integer>(20, 2),
            new Pair<Integer, Integer>(30, 3)));
    // #PassThroughTuple
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

// #PassThrough
class PassThroughFlow {

  public static <A, T> Graph<FlowShape<A, Pair<T, A>>, NotUsed> create(
      Flow<A, T, NotUsed> flow) {
    return create(flow, Keep.both());
  }

  public static <A, T, O> Graph<FlowShape<A, O>, NotUsed> create(
      Flow<A, T, NotUsed> flow, Function2<T, A, O> output) {
    return Flow.fromGraph(
        GraphDSL.create(
            builder -> {
              UniformFanOutShape<A, A> broadcast = builder.add(Broadcast.create(2));
              FanInShape2<T, A, O> zip = builder.add(ZipWith.create(output));
              builder.from(broadcast.out(0)).via(builder.add(flow)).toInlet(zip.in0());
              builder.from(broadcast.out(1)).toInlet(zip.in1());
              return FlowShape.apply(broadcast.in(), zip.out());
            }));
  }
}
// #PassThrough

class PassThroughFlowKafkaCommitExample {
  private static ActorSystem system;
  private static Materializer materializer;

  public void dummy() {
    // #passThroughKafkaFlow
    Flow<ConsumerMessage.CommittableMessage<String, byte[]>, String, NotUsed> writeFlow =
        Flow.fromFunction(i -> i.record().value().toString());

    ConsumerSettings<String, byte[]> consumerSettings =
        ConsumerSettings.create(system, new StringDeserializer(), new ByteArrayDeserializer());
    Consumer.DrainingControl<Done> control =
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .via(PassThroughFlow.create(writeFlow, Keep.right()))
            .map(i -> i.committableOffset())
            .groupedWithin(10, Duration.ofSeconds(5))
            .mapAsync(3, i -> i.commitJavadsl())
            .toMat(Sink.ignore(), Keep.both())
            .mapMaterializedValue(Consumer::createDrainingControl)
            .run(materializer);

    // #passThroughKafkaFlow
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }
}
