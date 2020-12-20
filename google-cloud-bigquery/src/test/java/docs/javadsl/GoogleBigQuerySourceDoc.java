/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #imports

import akka.actor.ActorSystem;
import akka.stream.Materializer;
// #imports

public class GoogleBigQuerySourceDoc {

  private static void example() {
    // #init-mat
    ActorSystem system = ActorSystem.create();
    Materializer materializer = Materializer.createMaterializer(system);
    // #init-mat

    // #init-config
    // TODO
    // #init-config

    // #list-tables-and-fields
    // TODO
    // #list-tables-and-fields

    // #csv-style
    // TODO
    // #csv-style
  }

  // #run-query
  // TODO
  // #run-query

  // #dry-run
  // TODO
  // #dry-run
}
