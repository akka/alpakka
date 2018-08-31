/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import javax.jms._
import javax.jms.{Destination => JmsDestination}
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

    val settings = JmsProducerSettings(factory, destination = Option(settingsDestination))
    val jmsSession = new JmsSession(connection, session, destination, settingsDestination)

    val jmsProducer = JmsMessageProducer(jmsSession, settings)
  }

  "populating Jms message properties" should {
    "succeed if properties are set to supported types" in new Setup {
      jmsProducer.populateMessageProperties(
        textMessage,
        JmsTextMessage("test",
                       Set.empty,
                       Map("string" -> "string",
                           "int" -> 1,
                           "boolean" -> true,
                           "byte" -> 2.toByte,
                           "short" -> 3.toShort,
                           "long" -> 4L,
                           "double" -> 5.0))
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
      assertThrows[UnsupportedMessagePropertyType] {
        val wrongProperties: Map[String, Any] = Map("object" -> this)
        jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test", Set.empty, wrongProperties))
      }
    }

    "fail if a property is set to a null value" in new Setup {
      assertThrows[NullMessageProperty] {
        val wrongProperties: Map[String, Any] = Map("object" -> null)
        jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test", Set.empty, wrongProperties))
      }
    }
  }

  "creating a Jms Map message" should {
    "succeed if map values are supported types" in new Setup {
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
      assertThrows[UnsupportedMapMessageEntryType] {
        val wrongMap: Map[String, Any] = Map("object" -> this)
        jmsProducer.createMessage(JmsMapMessage(wrongMap))
      }
    }

    "fail if a map value is set to null" in new Setup {
      assertThrows[NullMapMessageEntry] {
        val wrongMap: Map[String, Any] = Map("object" -> null)
        jmsProducer.createMessage(JmsMapMessage(wrongMap))
      }
    }
  }
}
