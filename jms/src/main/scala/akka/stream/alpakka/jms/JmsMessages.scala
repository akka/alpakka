/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import scala.collection.JavaConverters._
import java.util

sealed trait JmsMessage {

  def properties(): Map[String, Any]

  /**
   * Java API: add  [[JmsMessage]]
   */
  def add(name: String, value: Any): JmsMessage
}

final case class JmsTextMessage(body: String, properties: Map[String, Any] = Map.empty) extends JmsMessage {

  override def add(name: String, value: Any): JmsTextMessage = copy(properties = properties + (name -> value))
}

final case class JmsByteMessage(bytes: Array[Byte], properties: Map[String, Any] = Map.empty) extends JmsMessage {

  override def add(name: String, value: Any): JmsByteMessage = copy(properties = properties + (name -> value))
}

final case class JmsMapMessage(body: Map[String, Any], properties: Map[String, Any] = Map.empty) extends JmsMessage {

  override def add(name: String, value: Any): JmsMapMessage = copy(properties = properties + (name -> value))

}

final case class JmsObjectMessage(serializable: java.io.Serializable, properties: Map[String, Any] = Map.empty)
    extends JmsMessage {

  override def add(name: String, value: Any): JmsObjectMessage = copy(properties = properties + (name -> value))
}

object JmsMessage {

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String) = JmsTextMessage(body, Map.empty)

  /**
   * Java API: create  [[JmsTextMessage]] with properties
   */
  def create(body: String, properties: util.Map[String, Any]) = JmsTextMessage(body, properties.asScala.toMap)

  /**
   * Java API: create  [[JmsByteMessage]]
   */
  def create(bytes: Array[Byte]) = JmsByteMessage(bytes, Map.empty)

  /**
   * Java API: create  [[JmsByteMessage]] with properties
   */
  def create(bytes: Array[Byte], properties: util.Map[String, Any]) = JmsByteMessage(bytes, properties.asScala.toMap)

  /**
   * Java API: create  [[JmsMapMessage]]
   */
  def create(map: Map[String, Any]) = JmsMapMessage(map, Map.empty)

  /**
   * Java API: create  [[JmsMapMessage]] with properties
   */
  def create(map: Map[String, Any], properties: util.Map[String, Any]) = JmsMapMessage(map, properties.asScala.toMap)

  /**
   * Java API: create  [[JmsMapMessage]]
   */
  def create(serializable: Serializable) = JmsObjectMessage(serializable, Map.empty)

  /**
   * Java API: create  [[JmsMapMessage]] with properties
   */
  def create(serializable: Serializable, properties: util.Map[String, Any]) =
    JmsObjectMessage(serializable, properties.asScala.toMap)

}
