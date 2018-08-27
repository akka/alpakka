/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.scaladsl
import java.time.Duration

import akka.stream.alpakka.jms.{Credentials, JmsProducerSettings, JmsSpec}
import org.apache.activemq.ActiveMQConnectionFactory

class JmsSettingsSpec extends JmsSpec {

  "Jms producer" should {
    "have producer settings" in {
      //#producer-settings
      val settings = JmsProducerSettings(new ActiveMQConnectionFactory("broker-url"))
        .withTopic("target-topic")
        .withCredential(Credentials("username", "password"))
        .withSessionCount(10)
        .withTimeToLive(Duration.ofHours(1))
      //#producer-settings
    }
  }
}
