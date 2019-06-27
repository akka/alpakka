/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import javax.jms

import scala.concurrent.TimeoutException
import scala.util.control.NoStackTrace

/**
 * Marker trait indicating that the exception thrown is persistent. The operation will always fail when retried.
 */
trait NonRetriableJmsException extends Exception

case class UnsupportedMessagePropertyType(propertyName: String, propertyValue: Any, message: JmsEnvelope[_])
    extends Exception(
      s"Jms property '$propertyName' has unknown type '${propertyValue.getClass.getName}'. " +
      "Only primitive types and String are supported as property values."
    )
    with NonRetriableJmsException

case class NullMessageProperty(propertyName: String, message: JmsEnvelope[_])
    extends Exception(
      s"null value was given for Jms property '$propertyName'."
    )
    with NonRetriableJmsException

case class UnsupportedMapMessageEntryType(entryName: String, entryValue: Any, message: JmsMapMessagePassThrough[_])
    extends Exception(
      s"Jms MapMessage entry '$entryName' has unknown type '${entryValue.getClass.getName}'. " +
      "Only primitive types, String, and Byte array are supported as entry values."
    )
    with NonRetriableJmsException

case class NullMapMessageEntry(entryName: String, message: JmsMapMessagePassThrough[_])
    extends Exception(
      s"null value was given for Jms MapMessage entry '$entryName'."
    )
    with NonRetriableJmsException

case class UnsupportedMessageType(message: jms.Message)
    extends Exception(
      s"Can't convert a ${message.getClass.getName} to a JmsMessage"
    )
    with NonRetriableJmsException

case class ConnectionRetryException(message: String, cause: Throwable) extends Exception(message, cause)

case object RetrySkippedOnMissingConnection
    extends Exception("JmsProducer is not connected, send attempt skipped")
    with NoStackTrace

final case class StopMessageListenerException() extends Exception("Stopping MessageListener.")

case object JmsNotConnected extends Exception("JmsConnector is not connected") with NoStackTrace

case class JmsConnectTimedOut(message: String) extends TimeoutException(message)
