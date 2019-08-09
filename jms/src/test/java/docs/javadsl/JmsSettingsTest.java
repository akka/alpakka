/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.jms.*;
import com.typesafe.config.ConfigFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

// #retry-settings #send-retry-settings
import com.typesafe.config.Config;
import scala.Option;

// #retry-settings #send-retry-settings

public class JmsSettingsTest {

  @Test
  public void producerSettings() throws Exception {

    Config config = ConfigFactory.load();
    // #retry-settings
    Config connectionRetryConfig = config.getConfig("alpakka.jms.connection-retry");
    // reiterating the values from reference.conf
    ConnectionRetrySettings retrySettings =
        ConnectionRetrySettings.create(connectionRetryConfig)
            .withConnectTimeout(Duration.ofSeconds(10))
            .withInitialRetry(Duration.ofMillis(100))
            .withBackoffFactor(2.0)
            .withMaxBackoff(Duration.ofMinutes(1))
            .withMaxRetries(10);
    // #retry-settings

    ConnectionRetrySettings retrySettings2 = ConnectionRetrySettings.create(connectionRetryConfig);
    assertEquals(retrySettings.toString(), retrySettings2.toString());

    // #send-retry-settings
    Config sendRetryConfig = config.getConfig("alpakka.jms.send-retry");
    // reiterating the values from reference.conf
    SendRetrySettings sendRetrySettings =
        SendRetrySettings.create(sendRetryConfig)
            .withInitialRetry(Duration.ofMillis(20))
            .withBackoffFactor(1.5d)
            .withMaxBackoff(Duration.ofMillis(500))
            .withMaxRetries(10);
    // #send-retry-settings
    SendRetrySettings sendRetrySettings2 = SendRetrySettings.create(sendRetryConfig);
    assertEquals(sendRetrySettings.toString(), sendRetrySettings2.toString());

    // #producer-settings
    Config producerConfig = config.getConfig(JmsProducerSettings.configPath());
    JmsProducerSettings settings =
        JmsProducerSettings.create(producerConfig, new ActiveMQConnectionFactory("broker-url"))
            .withTopic("target-topic")
            .withCredentials(Credentials.create("username", "password"))
            .withConnectionRetrySettings(retrySettings)
            .withSendRetrySettings(sendRetrySettings)
            .withSessionCount(10)
            .withTimeToLive(Duration.ofHours(1));
    // #producer-settings
  }

  @Test
  public void consumerSettings() throws Exception {
    Config config = ConfigFactory.load();
    Config connectionRetryConfig = config.getConfig("alpakka.jms.connection-retry");
    ConnectionRetrySettings retrySettings = ConnectionRetrySettings.create(connectionRetryConfig);

    // #consumer-settings
    Config consumerConfig = config.getConfig(JmsConsumerSettings.configPath());
    JmsConsumerSettings settings =
        JmsConsumerSettings.create(consumerConfig, new ActiveMQConnectionFactory("broker-url"))
            .withTopic("message-topic")
            .withCredentials(Credentials.create("username", "password"))
            .withConnectionRetrySettings(retrySettings)
            .withSessionCount(10)
            .withAcknowledgeMode(AcknowledgeMode.AutoAcknowledge())
            .withSelector("Important = TRUE");
    // #consumer-settings
    assertThat(settings.sessionCount(), is(10));
    assertThat(settings.acknowledgeMode(), is(Option.apply(AcknowledgeMode.AutoAcknowledge())));
  }
}
