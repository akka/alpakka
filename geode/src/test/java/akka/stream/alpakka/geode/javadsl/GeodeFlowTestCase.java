/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;



public class GeodeFlowTestCase extends GeodeBaseTestCase {


    @Test
    public void flow() throws ExecutionException, InterruptedException {

        ReactiveGeode reactiveGeode = createReactiveGeode();

        Source<Person, NotUsed> source = buildPersonsSource(110, 111, 113, 114, 115);

        //#flow
        Flow<Person, Person, NotUsed> flow = reactiveGeode.flow(personRegionSettings, new PersonPdxSerializer());

        CompletionStage<List<Person>> run = source
                .via(flow)
                .toMat(Sink.seq(), Keep.right())
                .run(materializer);
        //#flow

        run.toCompletableFuture().get();

        reactiveGeode.close();

    }


}