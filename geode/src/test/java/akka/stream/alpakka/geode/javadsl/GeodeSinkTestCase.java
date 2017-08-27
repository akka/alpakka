/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class GeodeSinkTestCase extends GeodeBaseTestCase {


    @Test
    public void sinkTest() throws ExecutionException, InterruptedException {

        ReactiveGeode reactiveGeode = createReactiveGeode();

        Sink<Person, CompletionStage<Done>> sink = reactiveGeode.sink(personRegionSettings, new PersonPdxSerializer());

        Source<Person, NotUsed> source = buildPersonsSource(100, 101, 103, 104, 105);

        RunnableGraph<CompletionStage<Done>> runnableGraph = source
                .toMat(sink, Keep.right());

        CompletionStage<Done> stage = runnableGraph.run(materializer);

        stage.toCompletableFuture().get();

        reactiveGeode.close();

    }

    @Test
    public void sinkAnimalTest() throws ExecutionException, InterruptedException {

        ReactiveGeode reactiveGeode = createReactiveGeode();

        Source<Animal, NotUsed> source = buildAnimalsSource(100, 101, 103, 104, 105);

        //#sink
        Sink<Animal, CompletionStage<Done>> sink = reactiveGeode.sink(animalRegionSettings, new AnimalPdxSerializer());

        RunnableGraph<CompletionStage<Done>> runnableGraph = source
                .toMat(sink, Keep.right());
        //#sink

        CompletionStage<Done> stage = runnableGraph.run(materializer);

        stage.toCompletableFuture().get();

        reactiveGeode.close();

    }


}