/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.stream.alpakka.jms.ConnectionRetrySettings;
import akka.stream.alpakka.jms.Credentials;
import akka.stream.alpakka.jms.JmsProducerSettings;
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
            .withMaxRetries(-1);
    // #retry-settings

    // #producer-settings
    JmsProducerSettings settings =
        JmsProducerSettings.create(new ActiveMQConnectionFactory("broker-url"))
            .withTopic("target-topic")
            .withCredential(new Credentials("username", "password"))
            .withConnectionRetrySettings(retrySettings)
            .withSessionCount(10)
            .withTimeToLive(Duration.ofHours(1));
    // #producer-settings
  }
}
