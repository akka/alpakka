/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.ironmq.impl.IronMqClient;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.FutureConverters;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class UnitTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private ActorSystem system;
  private Materializer materializer;
  private IronMqClient ironMqClient;

  @Before
  public void setup() throws Exception {
    Config config = initConfig();
    system = ActorSystem.create("TestActorSystem", config);
    materializer = Materializer.matFromSystem(system);
    ironMqClient =
        new IronMqClient(
            IronMqSettings.create(config.getConfig(IronMqSettings.ConfigPath())),
            system,
            materializer);
  }

  @After
  public void teardown() throws Exception {
    materializer.shutdown();
    TestKit.shutdownActorSystem(system);
  }

  protected Config initConfig() {
    String projectId = "project-" + System.currentTimeMillis();
    return ConfigFactory.parseString("alpakka.ironmq.credentials.project-id = " + projectId)
        .withFallback(ConfigFactory.load());
  }

  protected ActorSystem getActorSystem() {
    if (system == null) throw new IllegalStateException("The ActorSystem is not yet initialized");
    return system;
  }

  protected Materializer getMaterializer() {
    if (materializer == null)
      throw new IllegalStateException("The Materializer is not yet initialized");
    return materializer;
  }

  public IronMqClient getIronMqClient() {
    if (ironMqClient == null)
      throw new IllegalStateException("The IronMqClient is not yet initialized");
    return ironMqClient;
  }

  protected String givenQueue() {
    return givenQueue("test-" + UUID.randomUUID());
  }

  protected String givenQueue(String name) {
    try {
      return FutureConverters.asJava(ironMqClient.createQueue(name, system.dispatcher()))
          .toCompletableFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Message.Ids givenMessages(String queueName, int n) {

    List<PushMessage> messages =
        IntStream.rangeClosed(1, n)
            .mapToObj(i -> PushMessage.create("test-" + i))
            .collect(Collectors.toList());

    try {
      return FutureConverters.asJava(
              ironMqClient.pushMessages(
                  queueName,
                  CollectionConverters.asScala(messages).toSeq(),
                  system.dispatcher()))
          .toCompletableFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
