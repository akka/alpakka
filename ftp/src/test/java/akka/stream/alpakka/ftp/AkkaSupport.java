/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp;

import akka.actor.ActorSystem;
import akka.stream.Materializer;

interface AkkaSupport {
  ActorSystem getSystem();

  Materializer getMaterializer();
}
