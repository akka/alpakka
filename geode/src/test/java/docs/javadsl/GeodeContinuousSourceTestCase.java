/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.alpakka.geode.javadsl.GeodeWithPoolSubscription;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class GeodeContinuousSourceTestCase extends GeodeBaseTestCase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void continuousSourceTest() throws ExecutionException, InterruptedException {

    GeodeWithPoolSubscription geode = createGeodeWithPoolSubscription();

    // #continuousQuery
    CompletionStage<Done> fut =
        geode
            .continuousQuery("test", "select * from /persons", new PersonPdxSerializer())
            .runForeach(
                p -> {
                  LOGGER.debug(p.toString());
                  if (p.getId() == 120) {
                    geode.closeContinuousQuery("test");
                  }
                },
                materializer);
    // #continuousQuery

    Flow<Person, Person, NotUsed> flow =
        geode.flow(personRegionSettings, new PersonPdxSerializer());

    Pair<NotUsed, CompletionStage<List<Person>>> run =
        Source.from(Arrays.asList(120))
            .map((i) -> new Person(i, String.format("Java flow %d", i), new Date()))
            .via(flow)
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    run.second().toCompletableFuture().get();

    fut.toCompletableFuture().get();

    geode.close();
  }
}
