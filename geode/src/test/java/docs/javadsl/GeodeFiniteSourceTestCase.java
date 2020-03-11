/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.geode.javadsl.Geode;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class GeodeFiniteSourceTestCase extends GeodeBaseTestCase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void finiteSourceTest() throws ExecutionException, InterruptedException {

    Geode geode = createGeodeClient();

    // #query
    CompletionStage<Done> personsDone =
        geode
            .query("select * from /persons", new PersonPdxSerializer())
            .runForeach(
                p -> {
                  LOGGER.debug(p.toString());
                },
                materializer);
    // #query

    personsDone.toCompletableFuture().get();

    CompletionStage<Done> animalsDone =
        geode
            .query("select * from /animals", new AnimalPdxSerializer())
            .runForeach(
                p -> {
                  LOGGER.debug(p.toString());
                },
                materializer);

    animalsDone.toCompletableFuture().get();
    geode.close();
  }
}
