/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms

sealed trait Destination {
  val name: String
  val create: jms.Session => jms.Destination
}

final case class Topic(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}

final case class DurableTopic(name: String, subscriberName: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createTopic(name)
}

final case class Queue(override val name: String) extends Destination {
  override val create: jms.Session => jms.Destination = session => session.createQueue(name)
}

final case class CustomDestination(override val name: String, override val create: jms.Session => jms.Destination)
    extends Destination
