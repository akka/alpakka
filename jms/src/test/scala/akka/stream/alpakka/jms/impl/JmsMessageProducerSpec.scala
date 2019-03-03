/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.stream.alpakka.jms.{Destination, _}
import javax.jms.{Destination => JmsDestination, _}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyString}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

class JmsMessageProducerSpec extends JmsSpec with MockitoSugar {

  trait Setup {
    val factory: ConnectionFactory = mock[ConnectionFactory]
    val connection: Connection = mock[Connection]
    val session: Session = mock[Session]
    val destination: JmsDestination = mock[JmsDestination]
    val producer: MessageProducer = mock[MessageProducer]
    val textMessage: TextMessage = mock[TextMessage]
    val mapMessage: MapMessage = mock[MapMessage]
    val settingsDestination: Destination = mock[Destination]

    when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
    when(session.createProducer(any[javax.jms.Destination])).thenReturn(producer)
    when(session.createTextMessage(anyString())).thenReturn(textMessage)
    when(session.createMapMessage()).thenReturn(mapMessage)

    val settings = JmsProducerSettings(producerConfig, factory).withDestination(settingsDestination)
    val jmsSession = new JmsProducerSession(connection, session, destination)

  }

  "populating Jms message properties" should {
    "succeed if properties are set to supported types" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      jmsProducer.populateMessageProperties(
        textMessage,
        JmsTextMessage("test")
          .withProperty("string", "string")
          .withProperty("int", 1)
          .withProperty("boolean", true)
          .withProperty("byte", 2.toByte)
          .withProperty("short", 3.toShort)
          .withProperty("long", 4L)
          .withProperty("double", 5.0)
      )

      verify(textMessage).setStringProperty("string", "string")
      verify(textMessage).setIntProperty("int", 1)
      verify(textMessage).setBooleanProperty("boolean", true)
      verify(textMessage).setByteProperty("byte", 2.toByte)
      verify(textMessage).setShortProperty("short", 3.toByte)
      verify(textMessage).setLongProperty("long", 4L)
      verify(textMessage).setDoubleProperty("double", 5.0)
    }

    "fail if a property is set to an unsupported type" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[UnsupportedMessagePropertyType] {
        jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test").withProperty("object", this))
      }
    }

    "fail if a property is set to a null value" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[NullMessageProperty] {
        jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test").withProperty("object", null))
      }
    }
  }

  "creating a Jms Map message" should {
    "succeed if map values are supported types" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      jmsProducer.createMessage(
        JmsMapMessage(
          Map("string" -> "string",
              "int" -> 1,
              "boolean" -> true,
              "byte" -> 2.toByte,
              "short" -> 3.toShort,
              "long" -> 4L,
              "double" -> 5.0)
        )
      )
      verify(mapMessage).setString("string", "string")
      verify(mapMessage).setInt("int", 1)
      verify(mapMessage).setBoolean("boolean", true)
      verify(mapMessage).setByte("byte", 2.toByte)
      verify(mapMessage).setShort("short", 3.toByte)
      verify(mapMessage).setLong("long", 4L)
      verify(mapMessage).setDouble("double", 5.0)
    }

    "fail if a map value is set to an unsupported type" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[UnsupportedMapMessageEntryType] {
        val wrongMap: Map[String, Any] = Map("object" -> this)
        jmsProducer.createMessage(JmsMapMessage(wrongMap))
      }
    }

    "fail if a map value is set to null" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[NullMapMessageEntry] {
        val wrongMap: Map[String, Any] = Map("object" -> null)
        jmsProducer.createMessage(JmsMapMessage(wrongMap))
      }
    }
  }
}
