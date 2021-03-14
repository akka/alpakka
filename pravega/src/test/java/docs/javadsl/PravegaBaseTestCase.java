/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.net.URI;
import java.util.UUID;

import akka.stream.alpakka.pravega.PravegaAkkaSpecSupport;
import akka.stream.alpakka.pravega.PravegaAkkaTestCaseSupport;
import akka.stream.alpakka.pravega.ReaderSettingsBuilder;

import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import akka.testkit.javadsl.TestKit;

import akka.stream.alpakka.pravega.ReaderSettings;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;

public abstract class PravegaBaseTestCase extends PravegaAkkaTestCaseSupport {

  protected String newGroup() {
    return "java-test-group-" + UUID.randomUUID().toString();
  }

  protected String newScope() {
    return "java-test-scope-" + UUID.randomUUID().toString();
  }

  protected String newStreamName() {
    return "java-test-topic-" + UUID.randomUUID().toString();
  }

  @BeforeClass
  public static void setup() {
    init();
  }

  public void createStream(String scope, String streamName) {
    StreamManager streamManager = StreamManager.create(URI.create("tcp://localhost:9090"));

    if (streamManager.createScope(scope)) LOGGER.info("Created scope [{}]", scope);
    else LOGGER.info("Scope [{}] already exists", scope);
    StreamConfiguration streamConfig =
        StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
    if (streamManager.createStream(scope, streamName, streamConfig))
      LOGGER.info("Created stream [{}] in scope [{}]", streamName, scope);
    else LOGGER.info("");

    streamManager.close();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }
}
