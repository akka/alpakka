/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}

case class PushMessage(body: String, delay: FiniteDuration = Duration.Zero)

object PushMessage {

  def create(body: String): PushMessage = PushMessage(body)

  def create(body: String, duration: java.time.Duration): PushMessage =
    PushMessage(body, FiniteDuration(duration.getSeconds, TimeUnit.SECONDS))
}

/**
 * The message consumed from IronMq.
 *
 * @param messageId The unique id of the message.
 * @param body The pushed message content.
 * @param noOfReservations It is the count of how many time the message has been reserved (and released or expired) previously
 */
case class Message(messageId: Message.Id, body: String, noOfReservations: Int)

object Message {

  case class Id(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class Ids(ids: List[Id])
}

/**
 * The message reserved from IronMq.
 *
 * This message has been ask to be reserved from IronMq. It contains both the message itself and the reservation id.
 *
 * @param reservationId The reservation id needed to release or delete the message.
 * @param message The fetched message.
 */
case class ReservedMessage(reservationId: Reservation.Id, message: Message) {
  val messageId: Message.Id = message.messageId
  val messageBody: String = message.body
  val reservation: Reservation = Reservation(messageId, reservationId)
}

/**
 * Represent a message reservation. It is used when you need to delete or release a reserved message. It is obtained from
 * a [[ReservedMessage]] by message id and reservation id.
 *
 * @param messageId The previously reserved message Id.
 * @param reservationId The reservation id
 */
case class Reservation(messageId: Message.Id, reservationId: Reservation.Id)

object Reservation {
  case class Id(value: String) extends AnyVal {
    override def toString: String = value
  }
}

/**
 * Simplified representation of the IronMq queue.
 *
 * @param name The name associated with this Queue.
 */
case class Queue(name: Queue.Name)

object Queue {

  case class Name(value: String) extends AnyVal {
    override def toString: String = value
  }
}
