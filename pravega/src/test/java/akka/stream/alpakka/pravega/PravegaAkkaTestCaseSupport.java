/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega;

import akka.actor.ActorSystem;
import docs.javadsl.PravegaBaseTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaAkkaTestCaseSupport {

  protected static final Logger LOGGER = LoggerFactory.getLogger(PravegaBaseTestCase.class);
  protected static ActorSystem system;

  public static void init() {
    system = ActorSystem.create();
  }
}
