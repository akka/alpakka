/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.jms._
import javax.jms

/**
 * Internal API.
 */
@InternalApi
private class JmsMessageProducer(jmsProducer: jms.MessageProducer, jmsSession: JmsProducerSession, val epoch: Int) {

  private val defaultDestination = jmsSession.jmsDestination

  private val destinationCache = new SoftReferenceCache[Destination, jms.Destination]()

  def send(elem: JmsEnvelope[_]): Unit = {
    val message: jms.Message = createMessage(elem)
    populateMessageProperties(message, elem)

    val (sendHeaders, headersBeforeSend: Set[JmsHeader]) = elem.headers.partition(_.usedDuringSend)
    populateMessageHeader(message, headersBeforeSend)

    val deliveryMode = sendHeaders
      .collectFirst { case x: JmsDeliveryMode => x.deliveryMode }
      .getOrElse(jmsProducer.getDeliveryMode)

    val priority = sendHeaders
      .collectFirst { case x: JmsPriority => x.priority }
      .getOrElse(jmsProducer.getPriority)

    val timeToLive = sendHeaders
      .collectFirst { case x: JmsTimeToLive => x.timeInMillis }
      .getOrElse(jmsProducer.getTimeToLive)

    val destination = elem.destination match {
      case Some(messageDestination) => lookup(messageDestination)
      case None => defaultDestination
    }
    jmsProducer.send(destination, message, deliveryMode, priority, timeToLive)
  }

  private def lookup(dest: Destination) = destinationCache.lookup(dest, dest.create(jmsSession.session))

  private[jms] def createMessage(element: JmsEnvelope[_]): jms.Message =
    element match {

      case textMessage: JmsTextMessagePassThrough[_] => jmsSession.session.createTextMessage(textMessage.body)

      case byteMessage: JmsByteMessagePassThrough[_] =>
        val newMessage = jmsSession.session.createBytesMessage()
        newMessage.writeBytes(byteMessage.bytes)
        newMessage

      case byteStringMessage: JmsByteStringMessagePassThrough[_] =>
        val newMessage = jmsSession.session.createBytesMessage()
        newMessage.writeBytes(byteStringMessage.bytes.toArray)
        newMessage

      case mapMessage: JmsMapMessagePassThrough[_] =>
        val newMessage = jmsSession.session.createMapMessage()
        populateMapMessage(newMessage, mapMessage)
        newMessage

      case objectMessage: JmsObjectMessagePassThrough[_] =>
        jmsSession.session.createObjectMessage(objectMessage.serializable)

      case pt: JmsPassThrough[_] => throw new IllegalArgumentException("can't create message for JmsPassThrough")

    }

  private[jms] def populateMessageProperties(message: javax.jms.Message, jmsMessage: JmsEnvelope[_]): Unit =
    jmsMessage.properties.foreach { case (key, v) =>
      v match {
        case v: String => message.setStringProperty(key, v)
        case v: Int => message.setIntProperty(key, v)
        case v: Boolean => message.setBooleanProperty(key, v)
        case v: Byte => message.setByteProperty(key, v)
        case v: Short => message.setShortProperty(key, v)
        case v: Float => message.setFloatProperty(key, v)
        case v: Long => message.setLongProperty(key, v)
        case v: Double => message.setDoubleProperty(key, v)
        case null => throw NullMessageProperty(key, jmsMessage)
        case _ => throw UnsupportedMessagePropertyType(key, v, jmsMessage)
      }
    }

  private def populateMapMessage(message: javax.jms.MapMessage, jmsMessage: JmsMapMessagePassThrough[_]): Unit =
    jmsMessage.body.foreach { case (key, v) =>
      v match {
        case v: String => message.setString(key, v)
        case v: Int => message.setInt(key, v)
        case v: Boolean => message.setBoolean(key, v)
        case v: Byte => message.setByte(key, v)
        case v: Short => message.setShort(key, v)
        case v: Float => message.setFloat(key, v)
        case v: Long => message.setLong(key, v)
        case v: Double => message.setDouble(key, v)
        case v: Array[Byte] => message.setBytes(key, v)
        case null => throw NullMapMessageEntry(key, jmsMessage)
        case _ => throw UnsupportedMapMessageEntryType(key, v, jmsMessage)
      }
    }

  private def populateMessageHeader(message: javax.jms.Message, headers: Set[JmsHeader]): Unit =
    headers.foreach {
      case JmsType(jmsType) => message.setJMSType(jmsType)
      case JmsReplyTo(destination) => message.setJMSReplyTo(destination.create(jmsSession.session))
      case JmsCorrelationId(jmsCorrelationId) => message.setJMSCorrelationID(jmsCorrelationId)
      case JmsExpiration(jmsExpiration) => message.setJMSExpiration(jmsExpiration)
      case JmsDeliveryMode(_) | JmsPriority(_) | JmsTimeToLive(_) | JmsTimestamp(_) | JmsRedelivered(_) |
          JmsMessageId(_) => // see #send(JmsMessage)
    }
}

/**
 * Internal API.
 */
@InternalApi
private[impl] object JmsMessageProducer {
  def apply(jmsSession: JmsProducerSession, settings: JmsProducerSettings, epoch: Int): JmsMessageProducer = {
    val producer = jmsSession.session.createProducer(null)
    if (settings.timeToLive.nonEmpty) {
      producer.setTimeToLive(settings.timeToLive.get.toMillis)
    }
    new JmsMessageProducer(producer, jmsSession, epoch)
  }
}
