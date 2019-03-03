/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

import akka.stream.javadsl.*;
import akka.stream.alpakka.slick.javadsl.*;

public class DocSnippetSink {
  public static void main(String[] args) throws Exception {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    final SlickSession session = SlickSession.forConfig("slick-h2");
    system.registerOnTermination(session::close);

    final List<User> users =
        IntStream.range(0, 42)
            .boxed()
            .map((i) -> new User(i, "Name" + i))
            .collect(Collectors.toList());

    // #sink-example
    final CompletionStage<Done> done =
        Source.from(users)
            .runWith(
                Slick.<User>sink(
                    session,
                    // add an optional second argument to specify the parallism factor (int)
                    (user) ->
                        "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES ("
                            + user.id
                            + ", '"
                            + user.name
                            + "')"),
                materializer);
    // #sink-example

    done.whenComplete(
        (value, exception) -> {
          system.terminate();
        });
  }
}
