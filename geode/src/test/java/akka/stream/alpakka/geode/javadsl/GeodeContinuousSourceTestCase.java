/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class GeodeContinuousSourceTestCase extends GeodeBaseTestCase {

    @Test
    public void continuousSourceTest() throws ExecutionException, InterruptedException {

        ReactiveGeodeWithPoolSubscription reactiveGeode = createReactiveGeodeWithPoolSubscription();

        //#continuousQuery
        CompletionStage<Done> fut = reactiveGeode.continuousQuery("test", "select * from /persons", new PersonPdxSerializer())
                .runForeach(p -> {
                    LOGGER.debug(p.toString());
                    if (p.getId() == 120) {
                        reactiveGeode.closeContinuousQuery("test");
                    }
                }, materializer);
        //#continuousQuery

        Flow<Person, Person, NotUsed> flow = reactiveGeode.flow(personRegionSettings, new PersonPdxSerializer());

        Pair<NotUsed, CompletionStage<List<Person>>> run = Source.from(Arrays.asList(120)).map((i)
                -> new Person(i, String.format("Java flow %d", i), new Date())).via(flow).toMat(Sink.seq(), Keep.both()).run(materializer);

        run.second().toCompletableFuture().get();


        fut.toCompletableFuture().get();

        reactiveGeode.close();

    }


}