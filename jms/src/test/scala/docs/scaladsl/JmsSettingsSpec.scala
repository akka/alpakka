/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.jms._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.OptionValues

import scala.concurrent.duration._

class JmsSettingsSpec extends JmsSpec with OptionValues {

  private val connectionFactory = new ActiveMQConnectionFactory("broker-url")

  "Jms producer" should {
    "have producer settings" in {

      //#retry-settings-case-class
      // reiterating defaults from reference.conf
      val retrySettings = ConnectionRetrySettings(system)
        .withConnectTimeout(10.seconds)
        .withInitialRetry(100.millis)
        .withBackoffFactor(2.0d)
        .withMaxBackoff(1.minute)
        .withMaxRetries(10)
      //#retry-settings-case-class

      //#send-retry-settings
      // reiterating defaults from reference.conf
      val sendRetrySettings = SendRetrySettings(system)
        .withInitialRetry(20.millis)
        .withBackoffFactor(1.5d)
        .withMaxBackoff(500.millis)
        .withMaxRetries(10)
      //#send-retry-settings

      //#producer-settings
      val producerConfig: Config = system.settings.config.getConfig(JmsProducerSettings.configPath)
      val settings = JmsProducerSettings(producerConfig, connectionFactory)
        .withTopic("target-topic")
        .withCredentials(Credentials("username", "password"))
        .withSessionCount(1)
      //#producer-settings

      val retrySettings2 = ConnectionRetrySettings(system)
      retrySettings.toString should be(retrySettings2.toString)

      val sendRetrySettings2 = SendRetrySettings(system)
      sendRetrySettings.toString should be(sendRetrySettings2.toString)

      val producerSettings2 = JmsProducerSettings(producerConfig, settings.connectionFactory)
        .withTopic("target-topic")
        .withCredentials(Credentials("username", "password"))
      settings.toString should be(producerSettings2.toString)

    }
  }

  "Jms consumer" should {
    "have consumer settings" in {

      val connectionRetryConfig: Config = system.settings.config.getConfig(ConnectionRetrySettings.configPath)
      val retrySettings = ConnectionRetrySettings(connectionRetryConfig)
        .withConnectTimeout(10.seconds)
        .withInitialRetry(100.millis)
        .withBackoffFactor(2.0d)
        .withMaxBackoff(1.minute)
        .withMaxRetries(10)

      //#consumer-settings
      val consumerConfig: Config = system.settings.config.getConfig(JmsConsumerSettings.configPath)
      // reiterating defaults from reference.conf
      val settings = JmsConsumerSettings(consumerConfig, connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))
        .withConnectionRetrySettings(retrySettings)
        .withSessionCount(1)
        .withBufferSize(100)
        .withAckTimeout(1.second)
      //#consumer-settings

      val retrySettings2 = ConnectionRetrySettings(system)
      retrySettings.toString should be(retrySettings2.toString)

      val consumerSettings2 = JmsConsumerSettings(consumerConfig, settings.connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))
      settings.toString should be(consumerSettings2.toString)
    }

    "read from user config" in {
      val config = ConfigFactory
        .parseString("""
          |connection-retry {
          |    connect-timeout = 10 seconds
          |    initial-retry = 100 millis
          |    backoff-factor = 2
          |    max-backoff = 1 minute
          |    # infinite, or positive integer
          |    max-retries = 10
          |}
          |credentials {
          |  username = "some text"
          |  password = "other text"
          |}
          |session-count = 10
          |buffer-size = 1000
          |selector = "some text" # optional
          |acknowledge-mode = duplicates-ok
          |ack-timeout = 5 second
        """.stripMargin)
        .withFallback(consumerConfig)
        .resolve()

      val settings = JmsConsumerSettings(config, connectionFactory)
      settings.credentials.value should be(Credentials("some text", "other text"))
      settings.sessionCount should be(10)
      settings.bufferSize should be(1000)
      settings.selector.value should be("some text")
      settings.acknowledgeMode.value should be(AcknowledgeMode.DupsOkAcknowledge)
      settings.ackTimeout should be(5.seconds)
    }

    "read numeric acknowledge mode" in {
      val config = ConfigFactory
        .parseString("""
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
          |acknowledge-mode = 42
          |ack-timeout = 1 second
        """.stripMargin)
        .withFallback(consumerConfig)
        .resolve()

      val settings = JmsConsumerSettings(config, connectionFactory)
      settings.acknowledgeMode.value should be(new AcknowledgeMode(42))
    }
  }

  "Browse settings" should {
    "read from config" in {
      val retrySettings = ConnectionRetrySettings(system)
      //#browse-settings
      val browseConfig: Config = system.settings.config.getConfig(JmsBrowseSettings.configPath)
      val settings = JmsBrowseSettings(browseConfig, connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))
        .withConnectionRetrySettings(retrySettings)
      //#browse-settings

      val settings2 = JmsBrowseSettings(browseConfig, settings.connectionFactory)
        .withQueue("target-queue")
        .withCredentials(Credentials("username", "password"))

      settings.toString should be(settings2.toString)
    }
  }
}
