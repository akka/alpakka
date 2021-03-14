/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.geode.javadsl.Geode;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class GeodeSinkTestCase extends GeodeBaseTestCase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void sinkTest() throws ExecutionException, InterruptedException {

    Geode geode = createGeodeClient();

    Sink<Person, CompletionStage<Done>> sink =
        geode.sink(personRegionSettings, new PersonPdxSerializer());

    Source<Person, NotUsed> source = buildPersonsSource(100, 101, 103, 104, 105);

    RunnableGraph<CompletionStage<Done>> runnableGraph = source.toMat(sink, Keep.right());

    CompletionStage<Done> stage = runnableGraph.run(system);

    stage.toCompletableFuture().get();

    geode.close();
  }

  @Test
  public void sinkAnimalTest() throws ExecutionException, InterruptedException {

    Geode geode = createGeodeClient();

    Source<Animal, NotUsed> source = buildAnimalsSource(100, 101, 103, 104, 105);

    // #sink
    Sink<Animal, CompletionStage<Done>> sink =
        geode.sink(animalRegionSettings, new AnimalPdxSerializer());

    RunnableGraph<CompletionStage<Done>> runnableGraph = source.toMat(sink, Keep.right());
    // #sink

    CompletionStage<Done> stage = runnableGraph.run(system);

    stage.toCompletableFuture().get();

    geode.close();
  }
}
