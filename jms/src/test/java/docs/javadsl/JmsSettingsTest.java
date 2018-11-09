/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.jms.ConnectionRetrySettings;
import akka.stream.alpakka.jms.Credentials;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.SendRetrySettings;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

// #retry-settings #send-retry-settings
import java.time.Duration;

// #retry-settings #send-retry-settings

public class JmsSettingsTest {

  @Test
  public void settings() throws Exception {

    // #retry-settings
    ConnectionRetrySettings retrySettings =
        ConnectionRetrySettings.create()
            .withConnectTimeout(Duration.ofSeconds(3))
            .withInitialRetry(Duration.ofSeconds(1))
            .withBackoffFactor(1.5)
            .withMaxBackoff(Duration.ofSeconds(30))
            .withInfiniteRetries();
    // #retry-settings

    // #send-retry-settings
    SendRetrySettings sendRetrySettings =
        SendRetrySettings.create()
            .withInitialRetry(Duration.ofSeconds(10))
            .withBackoffFactor(2)
            .withMaxBackoff(Duration.ofSeconds(1))
            .withMaxRetries(60);
    // #send-retry-settings

    // #producer-settings
    JmsProducerSettings settings =
        JmsProducerSettings.create(new ActiveMQConnectionFactory("broker-url"))
            .withTopic("target-topic")
            .withCredential(new Credentials("username", "password"))
            .withConnectionRetrySettings(retrySettings)
            .withSendRetrySettings(sendRetrySettings)
            .withSessionCount(10)
            .withTimeToLive(Duration.ofHours(1));
    // #producer-settings
  }
}
