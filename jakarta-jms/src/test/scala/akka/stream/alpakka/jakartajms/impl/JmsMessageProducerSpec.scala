/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.impl

import akka.stream.alpakka.jakartajms.{Destination, _}
import jakarta.jms.{Destination => JmsDestination, _}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyString}
import org.mockito.Mockito._

class JmsMessageProducerSpec extends JmsSpec {

  trait Setup {
    val factory: ConnectionFactory = mock(classOf[ConnectionFactory])
    val connection: Connection = mock(classOf[Connection])
    val session: Session = mock(classOf[Session])
    val destination: JmsDestination = mock(classOf[JmsDestination])
    val producer: MessageProducer = mock(classOf[MessageProducer])
    val textMessage: TextMessage = mock(classOf[TextMessage])
    val mapMessage: MapMessage = mock(classOf[MapMessage])
    val settingsDestination: Destination = mock(classOf[Destination])

    when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session)
    when(session.createProducer(any[jakarta.jms.Destination])).thenReturn(producer)
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
          .withProperty("bytearray", Array[Byte](1, 1, 0))
      )

      verify(textMessage).setStringProperty("string", "string")
      verify(textMessage).setIntProperty("int", 1)
      verify(textMessage).setBooleanProperty("boolean", true)
      verify(textMessage).setByteProperty("byte", 2.toByte)
      verify(textMessage).setShortProperty("short", 3.toByte)
      verify(textMessage).setLongProperty("long", 4L)
      verify(textMessage).setDoubleProperty("double", 5.0)
      verify(textMessage).setObjectProperty("bytearray", Array[Byte](1, 1, 0))
    }

    "succeed if properties are set as map" in new Setup {
      val props = Map[String, Any](
        "string" -> "string",
        "int" -> 1,
        "boolean" -> true,
        "byte" -> 2.toByte,
        "short" -> 3.toShort,
        "float" -> 4.78f,
        "Java-boxed float" -> java.lang.Float.valueOf(4.35f),
        "long" -> 4L,
        "Java-boxed long" -> java.lang.Long.valueOf(44L),
        "double" -> 5.0,
        "bytearray" -> Array[Byte](1, 1, 0)
      )

      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      jmsProducer.populateMessageProperties(
        textMessage,
        JmsTextMessage("test")
          .withProperties(props)
      )

      verify(textMessage).setStringProperty("string", "string")
      verify(textMessage).setIntProperty("int", 1)
      verify(textMessage).setBooleanProperty("boolean", true)
      verify(textMessage).setByteProperty("byte", 2.toByte)
      verify(textMessage).setShortProperty("short", 3.toByte)
      verify(textMessage).setFloatProperty("float", 4.78f)
      verify(textMessage).setFloatProperty("Java-boxed float", 4.35f)
      verify(textMessage).setLongProperty("long", 4L)
      verify(textMessage).setLongProperty("Java-boxed long", 44L)
      verify(textMessage).setDoubleProperty("double", 5.0)
      verify(textMessage).setObjectProperty("bytearray", Array[Byte](1, 1, 0))
    }

    "fail if a property is set to an unsupported type" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[UnsupportedMessagePropertyType] {
        jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test").withProperty("object", this))
      }
    }

    "succeed if a property is set to a null value" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      jmsProducer.populateMessageProperties(textMessage, JmsTextMessage("test").withProperty("object", null))
      verify(textMessage).setObjectProperty("object", null)
    }
  }

  "creating a Jms Map message" should {
    "succeed if map values are supported types" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      jmsProducer.createMessage(
        JmsMapMessage(
          Map[String, Any](
            "string" -> "string",
            "int" -> 1,
            "boolean" -> true,
            "byte" -> 2.toByte,
            "short" -> 3.toShort,
            "float" -> 4.89f,
            "Java-boxed float" -> java.lang.Float.valueOf(4.35f),
            "long" -> 4L,
            "Java-boxed long" -> java.lang.Long.valueOf(44L),
            "double" -> 5.0
          )
        )
      )
      verify(mapMessage).setString("string", "string")
      verify(mapMessage).setInt("int", 1)
      verify(mapMessage).setBoolean("boolean", true)
      verify(mapMessage).setByte("byte", 2.toByte)
      verify(mapMessage).setShort("short", 3.toByte)
      verify(mapMessage).setFloat("float", 4.89f)
      verify(mapMessage).setFloat("Java-boxed float", 4.35f)
      verify(mapMessage).setLong("long", 4L)
      verify(mapMessage).setLong("Java-boxed long", 44L)
      verify(mapMessage).setDouble("double", 5.0)
    }

    "fail if a map value is set to an unsupported type" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      assertThrows[UnsupportedMapMessageEntryType] {
        val wrongMap: Map[String, Any] = Map("object" -> this)
        jmsProducer.createMessage(JmsMapMessage(wrongMap))
      }
    }

    "succeed if a map value is set to null" in new Setup {
      val jmsProducer = JmsMessageProducer(jmsSession, settings, 0)
      val correctMap: Map[String, Any] = Map("object" -> null)
      jmsProducer.createMessage(JmsMapMessage(correctMap))
      verify(mapMessage).setObject("object", null)
    }
  }
}
