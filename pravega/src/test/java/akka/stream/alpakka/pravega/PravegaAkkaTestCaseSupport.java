/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import docs.javadsl.PravegaBaseTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaAkkaTestCaseSupport {

  protected static final Logger LOGGER = LoggerFactory.getLogger(PravegaBaseTestCase.class);
  protected static ActorSystem system;
  protected static Materializer materializer;

  public static void init() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }
}
