/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import com.typesafe.config.ConfigFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

// #retry-settings #send-retry-settings
import akka.stream.alpakka.jms.ConnectionRetrySettings;
import akka.stream.alpakka.jms.Credentials;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.SendRetrySettings;
import com.typesafe.config.Config;

// #retry-settings #send-retry-settings

public class JmsSettingsTest {

  @Test
  public void consumerSettings() {}

  @Test
  public void settings() throws Exception {

    Config config = ConfigFactory.load();
    // #retry-settings
    Config connectionRetryConfig = config.getConfig("alpakka.jms.connection-retry");
    ConnectionRetrySettings retrySettings =
        ConnectionRetrySettings.create(connectionRetryConfig)
            .withConnectTimeout(Duration.ofSeconds(3))
            .withInitialRetry(Duration.ofSeconds(1))
            .withBackoffFactor(1.5)
            .withMaxBackoff(Duration.ofSeconds(30))
            .withInfiniteRetries();
    // #retry-settings

    ConnectionRetrySettings retrySettings2 = ConnectionRetrySettings.create(connectionRetryConfig);
    assertEquals(retrySettings.toString(), retrySettings2.toString());

    // #send-retry-settings
    Config sendRetryConfig = config.getConfig("alpakka.jms.send-retry");
    SendRetrySettings sendRetrySettings =
        SendRetrySettings.create(sendRetryConfig)
            .withInitialRetry(Duration.ofSeconds(10))
            .withBackoffFactor(2)
            .withMaxBackoff(Duration.ofSeconds(1))
            .withMaxRetries(60);
    // #send-retry-settings
    SendRetrySettings sendRetrySettings2 = SendRetrySettings.create(sendRetryConfig);
    assertEquals(sendRetrySettings.toString(), sendRetrySettings2.toString());

    // #producer-settings
    JmsProducerSettings settings =
        JmsProducerSettings.create(new ActiveMQConnectionFactory("broker-url"))
            .withTopic("target-topic")
            .withCredential(Credentials.create("username", "password"))
            .withConnectionRetrySettings(retrySettings)
            .withSendRetrySettings(sendRetrySettings)
            .withSessionCount(10)
            .withTimeToLive(Duration.ofHours(1));
    // #producer-settings
  }
}
