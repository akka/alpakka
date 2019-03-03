/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.geode.javadsl.Geode;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class GeodeFiniteSourceTestCase extends GeodeBaseTestCase {

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
