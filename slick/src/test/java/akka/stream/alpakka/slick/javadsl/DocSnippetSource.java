/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package example;

//#important-imports
import akka.stream.javadsl.*;
import akka.stream.alpakka.slick.javadsl.*;
//#important-imports

//#source-example
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

import akka.stream.javadsl.*;
import akka.stream.alpakka.slick.javadsl.*;

public class DocSnippetSource {
  public static void main(String[] args) throws Exception {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    final SlickSession session = SlickSession.forConfig("slick-h2");

    final CompletionStage<Done> done =
      Slick
        .source(
          session,
          "SELECT ID, NAME FROM ALPAKKA_SLICK_JAVADSL_TEST_USERS",
          (SlickRow row) -> new User(row.nextInt(), row.nextString())
        )
        .log("user")
        .runWith(Sink.ignore(), materializer);

    done.whenComplete((value, exception) -> {
      session.close();
      system.terminate();
    });
  }
}
//#source-example
