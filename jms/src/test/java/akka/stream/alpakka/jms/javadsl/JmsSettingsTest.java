/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.stream.alpakka.jms.Credentials;
import akka.stream.alpakka.jms.JmsProducerSettings;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import java.time.Duration;

public class JmsSettingsTest {

  @Test
  public void settings() throws Exception {
    // #producer-settings
    JmsProducerSettings settings =
        JmsProducerSettings.create(new ActiveMQConnectionFactory("broker-url"))
            .withTopic("target-topic")
            .withCredential(new Credentials("username", "password"))
            .withSessionCount(10)
            .withTimeToLive(Duration.ofHours(1));
    // #producer-settings
  }
}
