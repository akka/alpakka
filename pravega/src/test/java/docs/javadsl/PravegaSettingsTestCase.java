/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.alpakka.pravega.ReaderSettings;
import akka.stream.alpakka.pravega.ReaderSettingsBuilder;
import akka.stream.alpakka.pravega.WriterSettings;
import akka.stream.alpakka.pravega.WriterSettingsBuilder;
import akka.testkit.javadsl.TestKit;

import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

public class PravegaSettingsTestCase {

  protected static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
  }

  @Test
  public void readerSettings() {

    // #reader-settings
    ReaderSettings<String> readerSettings =
        ReaderSettingsBuilder.create(system)
            .clientConfigBuilder(
                builder -> builder.enableTlsToController(true)) // ClientConfig customization
            .readerConfigBuilder(
                builder -> builder.disableTimeWindows(true)) // ReaderConfig customization
            .withTimeout(Duration.ofSeconds(3))
            .withSerializer(new UTF8StringSerializer());
    // #reader-settings

    Assert.assertEquals("Timeout value doesn't match", readerSettings.timeout(), 3000);
    Assert.assertTrue(
        "TLS does not match", readerSettings.clientConfig().isEnableTlsToController());
    Assert.assertTrue(
        "Window should not be enabled", readerSettings.readerConfig().isDisableTimeWindows());
  }

  @Test
  public void writerSettings() {

    // #writer-settings
    WriterSettings<String> writerSettings =
        WriterSettingsBuilder.<String>create(system)
            .withKeyExtractor((String str) -> str.substring(0, 1))
            .withSerializer(new UTF8StringSerializer());
    // #writer-settings

    Assert.assertEquals(
        "Default value doesn't match", writerSettings.maximumInflightMessages(), 10);
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }
}
