/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.impl

import jakarta.jms

import akka.annotation.InternalApi
import akka.stream.alpakka.jakartajms._
import akka.util.ByteString
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

@InternalApi
private[jakartajms] object JmsMessageReader {

  /**
   * Read a [[akka.util.ByteString]] from a [[jakarta.jms.BytesMessage]]
   */
  def readBytes(message: jms.BytesMessage, bufferSize: Int = 4096): ByteString = {
    if (message.getBodyLength > Int.MaxValue)
      sys.error(s"Message too large, unable to read ${message.getBodyLength} bytes of data")

    val buff = new Array[Byte](Math.min(message.getBodyLength, bufferSize).toInt)

    @tailrec def read(data: ByteString): ByteString =
      if (message.getBodyLength == data.length)
        data
      else {
        val len = message.readBytes(buff)
        val d = buff.take(len)
        read(data ++ ByteString(d))
      }
    read(ByteString.empty)
  }

  /**
   * Read a byte array from a [[jakarta.jms.BytesMessage]]
   */
  def readArray(message: jms.BytesMessage, bufferSize: Int = 4096): Array[Byte] =
    readBytes(message, bufferSize).toArray

  private def createMap(keys: java.util.Enumeration[_], accessor: String => AnyRef) =
    keys
      .asInstanceOf[java.util.Enumeration[String]]
      .asScala
      .map { key =>
        key -> (accessor(key) match {
          case v: java.lang.Boolean => v.booleanValue()
          case v: java.lang.Byte => v.byteValue()
          case v: java.lang.Short => v.shortValue()
          case v: java.lang.Integer => v.intValue()
          case v: java.lang.Long => v.longValue()
          case v: java.lang.Float => v.floatValue()
          case v: java.lang.Double => v.doubleValue()
          case other => other
        })
      }
      .toMap

  /**
   * Read a Scala Map from a [[jakarta.jms.MapMessage]]
   */
  def readMap(message: jms.MapMessage): Map[String, Any] =
    createMap(message.getMapNames, message.getObject)

  /**
   * Extract a properties map from a [[jakarta.jms.Message]]
   */
  def readProperties(message: jms.Message): Map[String, Any] =
    createMap(message.getPropertyNames, message.getObjectProperty)

  /**
   * Extract [[JmsHeader]]s from a [[jakarta.jms.Message]]
   */
  def readHeaders(message: jms.Message): Set[JmsHeader] = {
    def messageId = Option(message.getJMSMessageID).map(JmsMessageId(_))
    def timestamp = Some(JmsTimestamp(message.getJMSTimestamp))
    def correlationId = Option(message.getJMSCorrelationID).map(JmsCorrelationId(_))
    def replyTo = Option(message.getJMSReplyTo).map(Destination(_)).map(JmsReplyTo(_))
    def deliveryMode = Some(JmsDeliveryMode(message.getJMSDeliveryMode))
    def redelivered = Some(JmsRedelivered(message.getJMSRedelivered))
    def jmsType = Option(message.getJMSType).map(JmsType(_))
    def expiration = Some(JmsExpiration(message.getJMSExpiration))
    def priority = Some(JmsPriority(message.getJMSPriority))

    Set(messageId, timestamp, correlationId, replyTo, deliveryMode, redelivered, jmsType, expiration, priority).flatten
  }
}
