/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.javadsl;

import akka.actor.ActorSystem;
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import com.typesafe.config.Config;

import java.util.concurrent.CompletionStage;

/**
 * Utility to delegate Couchbase node address lookup to [[https://doc.akka.io/docs/akka/current/discovery/index.html Akka Discovery]].
 */
public final class DiscoverySupport {

  private static final akka.stream.alpakka.couchbase.scaladsl.DiscoverySupport SUPPORT =
      akka.stream.alpakka.couchbase.scaladsl.DiscoverySupport.INSTANCE();

  /**
   * Expects a `service` section in the given Config and reads the given service name's address to
   * be used as Couchbase `nodes`.
   */
  public static final java.util.function.Function<
          CouchbaseSessionSettings, CompletionStage<CouchbaseSessionSettings>>
      getNodes(Config config, ActorSystem system) {
    return SUPPORT.getNodes(config, system);
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address to
   * be used as Couchbase `nodes`.
   */
  public static final java.util.function.Function<
          CouchbaseSessionSettings, CompletionStage<CouchbaseSessionSettings>>
      getNodes(ActorSystem system) {
    return SUPPORT.getNodes(
        system.settings().config().getConfig(CouchbaseSessionSettings.configPath()), system);
  }

  private DiscoverySupport() {}
}
