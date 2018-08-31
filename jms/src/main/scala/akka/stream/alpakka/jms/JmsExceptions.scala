/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

/**
 * Marker trait indicating that the exception thrown is persistent. The operation will always fail when retried.
 */
trait NonRetriableJmsException extends Exception

case class UnsupportedMessagePropertyType(propertyName: String, propertyValue: Any, message: JmsMessage)
    extends Exception(
      s"Jms property '$propertyName' has unknown type '${propertyValue.getClass.getName}'. " +
      "Only primitive types and String are supported as property values."
    )
    with NonRetriableJmsException

case class NullMessageProperty(propertyName: String, message: JmsMessage)
    extends Exception(
      s"null value was given for Jms property '$propertyName'."
    )
    with NonRetriableJmsException

case class UnsupportedMapMessageEntryType(entryName: String, entryValue: Any, message: JmsMapMessage)
    extends Exception(
      s"Jms MapMessage entry '$entryName' has unknown type '${entryValue.getClass.getName}'. " +
      "Only primitive types, String, and Byte array are supported as entry values."
    )
    with NonRetriableJmsException

case class NullMapMessageEntry(entryName: String, message: JmsMapMessage)
    extends Exception(
      s"null value was given for Jms MapMessage entry '$entryName'."
    )
    with NonRetriableJmsException
