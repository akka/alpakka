/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.geode.javadsl.Geode;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;

public class GeodeFlowTestCase extends GeodeBaseTestCase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void flow() throws ExecutionException, InterruptedException {

    Geode geode = createGeodeClient();

    Source<Person, NotUsed> source = buildPersonsSource(110, 111, 113, 114, 115);

    // #flow
    Flow<Person, Person, NotUsed> flow =
        geode.flow(personRegionSettings, new PersonPdxSerializer());

    CompletionStage<List<Person>> run =
        source.via(flow).toMat(Sink.seq(), Keep.right()).run(system);
    // #flow

    run.toCompletableFuture().get();

    geode.close();
  }
}
