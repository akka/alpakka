/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DocSnippetFlow {
  public static void main(String[] args) throws Exception {
    final ActorSystem system = ActorSystem.create();

    // #flow-example
    final SlickSession session = SlickSession.forConfig("slick-h2");
    system.registerOnTermination(session::close);

    final List<User> users =
        IntStream.range(0, 42)
            .boxed()
            .map((i) -> new User(i, "Name" + i))
            .collect(Collectors.toList());

    int parallelism = 1;

    final CompletionStage<Done> done =
        Source.from(users)
            .via(
                Slick.flow(
                    session,
                    parallelism,
                    (user, connection) -> {
                      PreparedStatement statement =
                          connection.prepareStatement(
                              "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES (?, ?)");
                      statement.setInt(1, user.id);
                      statement.setString(2, user.name);
                      return statement;
                    }))
            .log("nr-of-updated-rows")
            .runWith(Sink.ignore(), system);
    // #flow-example

    done.whenComplete(
        (value, exception) -> {
          system.terminate();
        });
  }
}
