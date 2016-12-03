/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import scala.concurrent.duration.{ Duration, FiniteDuration }

case class PushMessage(body: String, delay: Duration = Duration.Zero)

object Message {

  case class Id(value: String) extends AnyVal {
    override def toString: String = value
  }

  case class Ids(ids: List[Id])
}

case class Message(messageId: Message.Id, body: String, noOfReservations: Int)

case class ReserveMessages(
    noOfMessages: Int = 1,
    timeout: Duration = Duration.Undefined,
    waitFor: FiniteDuration = Duration.Zero,
    delete: Boolean = false
)

object Reservation {
  case class Id(value: String) extends AnyVal {
    override def toString: String = value
  }
}

case class Reservation(messageId: Message.Id, reservationId: Reservation.Id)

case class ReservedMessage(reservationId: Reservation.Id, message: Message) {
  val messageId: Message.Id = message.messageId
  val messageBody: String = message.body
  val reservation: Reservation = Reservation(messageId, reservationId)
}

case class Queue(name: Queue.Name)

object Queue {

  case class Name(value: String) extends AnyVal {
    override def toString: String = value
  }
}
