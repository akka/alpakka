/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms
import scala.compat.java8.FunctionConverters._

/**
 * A destination to send to/receive from.
 */
sealed abstract class Destination {
  val name: String
  val create: jms.Session => jms.Destination
}

object Destination {

  /**
   * Create a [[Destination]] from a [[javax.jms.Destination]]
   */
  def apply(destination: jms.Destination): Destination = destination match {
    case queue: jms.Queue => Queue(queue.getQueueName)
    case topic: jms.Topic => Topic(topic.getTopicName)
    case _ => CustomDestination(destination.toString, _ => destination)
  }

  /**
   * Java API: Create a [[Destination]] from a [[javax.jms.Destination]]
   */
  def createDestination(destination: jms.Destination): Destination = apply(destination)
}

/**
 * Specify a topic as destination to send to/receive from.
 */
final case class Topic(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}

/**
 * Specify a durable topic destination to send to/receive from.
 */
final case class DurableTopic(name: String, subscriberName: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}

/**
 * Specify a queue as destination to send to/receive from.
 */
final case class Queue(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createQueue(name)
}

/**
 * Destination factory to create specific destinations to send to/receive from.
 */
final case class CustomDestination(override val name: String, override val create: jms.Session => jms.Destination)
    extends Destination {

  /** Java API */
  def this(name: String, create: java.util.function.Function[jms.Session, jms.Destination]) =
    this(name, create.asScala)
}
