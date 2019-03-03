/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;

import akka.stream.ActorMaterializer;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.time.Duration;
import java.util.Optional;

// #event-source
import java.util.function.Function;
import java.util.concurrent.CompletionStage;

import akka.http.javadsl.Http;
import akka.http.javadsl.model.*;
import akka.http.javadsl.model.sse.ServerSentEvent;
import akka.stream.alpakka.sse.javadsl.EventSource;
// #event-source

public class EventSourceTest {

  @SuppressWarnings("ConstantConditions")
  public static void compileTest() {

    String host = "localhost";
    int port = 8080;
    ActorSystem system = null;
    ActorMaterializer materializer = null;

    int nrOfSamples = 10;

    // #event-source

    final Http http = Http.get(system);
    Function<HttpRequest, CompletionStage<HttpResponse>> send =
        (request) -> http.singleRequest(request);

    final Uri targetUri = Uri.create(String.format("http://%s:%d", host, port));
    final Optional<String> lastEventId = Optional.of("2");
    Source<ServerSentEvent, NotUsed> eventSource =
        EventSource.create(targetUri, send, lastEventId, materializer);
    // #event-source

    // #consume-events
    int elements = 1;
    Duration per = Duration.ofMillis(500);
    int maximumBurst = 1;

    eventSource
        .throttle(elements, per, maximumBurst, ThrottleMode.shaping())
        .take(nrOfSamples)
        .runWith(Sink.seq(), materializer);
    // #consume-events
  }
}
