/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.jms._
import org.apache.activemq.ActiveMQConnectionFactory

import scala.concurrent.duration._

class JmsSettingsSpec extends JmsSpec {

  "Jms producer" should {
    "have producer settings" in {

      //#retry-settings-case-class
      val retrySettings = ConnectionRetrySettings(
        connectTimeout = 3.seconds,
        initialRetry = 1.second,
        backoffFactor = 1.5,
        maxBackoff = 30.seconds,
        maxRetries = -1
      )
      //#retry-settings-case-class

      //#send-retry-settings
      val sendRetrySettings = SendRetrySettings()
        .withInitialRetry(10.millis)
        .withBackoffFactor(2)
        .withMaxBackoff(1.second)
        .withMaxRetries(60)
      //#send-retry-settings

      //#producer-settings
      val settings = JmsProducerSettings(new ActiveMQConnectionFactory("broker-url"))
        .withTopic("target-topic")
        .withCredential(Credentials("username", "password"))
        .withConnectionRetrySettings(retrySettings)
        .withSendRetrySettings(sendRetrySettings)
        .withSessionCount(10)
        .withTimeToLive(1.hour)
      //#producer-settings
    }
  }

  "Jms consumer" should {
    "have consumer settings" in {

      //#retry-settings-with-clause
      val retrySettings = ConnectionRetrySettings()
        .withConnectTimeout(3.seconds)
        .withInitialRetry(1.second)
        .withBackoffFactor(1.5)
        .withMaxBackoff(30.seconds)
        .withMaxRetries(-1)
      //#retry-settings-with-clause

      //#consumer-settings
      val settings = JmsConsumerSettings(new ActiveMQConnectionFactory("broker-url"))
        .withQueue("target-queue")
        .withCredential(Credentials("username", "password"))
        .withConnectionRetrySettings(retrySettings)
        .withSessionCount(10)
      //#consumer-settings
    }
  }
}
