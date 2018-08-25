package akka.stream.alpakka.jms.scaladsl
import java.time.Duration

import akka.stream.alpakka.jms.{Credentials, JmsProducerSettings, JmsSpec, Topic}
import org.apache.activemq.ActiveMQConnectionFactory

class JmsSettingsSpec extends JmsSpec {

  "Jms producer" should {
    "have producer settings" in {
      //#producer-settings
      val settings = JmsProducerSettings(
        new ActiveMQConnectionFactory("broker-url"),
        destination = Some(Topic("target-topic")),
        credentials = Some(Credentials("username", "password")),
        sessionCount = 10,
        timeToLive = Some(Duration.ofHours(1))
      )
      //#producer-settings
    }
  }
}
