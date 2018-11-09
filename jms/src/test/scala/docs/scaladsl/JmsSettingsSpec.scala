/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.jms._
import com.typesafe.config.ConfigFactory
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.OptionValues

import scala.concurrent.duration._

class JmsSettingsSpec extends JmsSpec with OptionValues {

  val consumerConfig = system.settings.config.getConfig(JmsConsumerSettings.configPath)
  val browseConfig = system.settings.config.getConfig(JmsBrowseSettings.configPath)

  "Jms producer" should {
    "have producer settings" in {

      //#retry-settings-case-class
      val retrySettings = ConnectionRetrySettings()
        .withConnectTimeout(3.seconds)
        .withInitialRetry(100.millis)
        .withBackoffFactor(2.0d)
        .withMaxBackoff(30.seconds)
        .withInfiniteRetries()
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
      // reiterating defaults from reference.conf
      val retrySettings = ConnectionRetrySettings(consumerConfig.getConfig("connection-retry"))
        .withConnectTimeout(10.seconds)
        .withInitialRetry(100.millis)
        .withBackoffFactor(2.0d)
        .withMaxBackoff(1.minute)
        .withMaxRetries(10)
      //#retry-settings-with-clause
      val retrySettings2 = ConnectionRetrySettings(consumerConfig.getConfig("connection-retry"))
      retrySettings.toString should be(retrySettings2.toString)

      //#consumer-settings
      // reiterating defaults from reference.conf
      val settings = JmsConsumerSettings(consumerConfig, new ActiveMQConnectionFactory("broker-url"))
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))
        .withConnectionRetrySettings(retrySettings)
        .withSessionCount(1)
        .withBufferSize(100)
        .withAckTimeout(1.second)
      //#consumer-settings

      val consumerSettings2 = JmsConsumerSettings(consumerConfig, settings.connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))
      settings.toString should be(consumerSettings2.toString)
    }

    "read from user config" in {
      val config = ConfigFactory.parseString("""
          |connection-retry {
          |    connect-timeout = 10 seconds
          |    initial-retry = 100 millis
          |    backoff-factor = 2
          |    max-backoff = 1 minute
          |    # infinite, or positive integer
          |    max-retries = 10
          |}
          |//    credentials {
          |//      username = "some text"
          |//      password = "some text"
          |//    }
          |session-count = 1
          |buffer-size = 100
          |// selector = "some text" # optional
          |acknowledge-mode = duplicates-ok
          |ack-timeout = 1 second
          |durable-name = "some text" # optional
        """.stripMargin).withFallback(consumerConfig).resolve()

      val settings = JmsConsumerSettings(config, new ActiveMQConnectionFactory("broker-url"))
      settings.acknowledgeMode.value should be(AcknowledgeMode.DupsOkAcknowledge)
      settings.durableName.value should be("some text")
    }
  }

  "Browse settings" should {
    "read from config" in {
      // reiterating defaults from reference.conf
      val settings = JmsBrowseSettings(browseConfig, new ActiveMQConnectionFactory("broker-url"))
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))

      val settings2 = JmsBrowseSettings(browseConfig, settings.connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))

      settings.toString should be (settings2.toString)
    }
  }
}
