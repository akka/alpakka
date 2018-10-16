/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.stream.alpakka.jms.ConnectionRetrySettings;
import akka.stream.alpakka.jms.Credentials;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.SendRetrySettings;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class JmsSettingsTest {

  @Test
  public void settings() throws Exception {

    // #retry-settings
    ConnectionRetrySettings retrySettings =
        ConnectionRetrySettings.create()
            .withConnectTimeout(3, TimeUnit.SECONDS)
            .withInitialRetry(1, TimeUnit.SECONDS)
            .withBackoffFactor(1.5)
            .withMaxBackoff(30, TimeUnit.SECONDS)
            .withInfiniteRetries();
    // #retry-settings

    // #send-retry-settings
    SendRetrySettings sendRetrySettings =
        SendRetrySettings.create()
            .withInitialRetry(10, TimeUnit.MILLISECONDS)
            .withBackoffFactor(2)
            .withMaxBackoff(1, TimeUnit.SECONDS)
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
