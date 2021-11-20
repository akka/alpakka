/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import akka.actor.ActorSystem;
import akka.stream.Materializer;

interface AkkaSupport {
  ActorSystem getSystem();

  Materializer getMaterializer();
}
