/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.ironmq.Message

/**
 * Internal API. The message reserved from IronMq.
 *
 * This message has been ask to be reserved from IronMq. It contains both the message itself and the reservation id.
 *
 * @param reservationId The reservation id needed to release or delete the message.
 * @param message The fetched message.
 */
@InternalApi
private[ironmq] case class ReservedMessage(reservationId: Reservation.Id, message: Message) {
  val messageId: Message.Id = message.messageId
  val messageBody: String = message.body
  val reservation: Reservation = Reservation(messageId, reservationId)
}

/**
 * Internal API.
 *
 * Represent a message reservation. It is used when you need to delete or release a reserved message. It is obtained from
 * a [[ReservedMessage]] by message id and reservation id.
 *
 * @param messageId The previously reserved message Id.
 * @param reservationId The reservation id
 */
@InternalApi
private[ironmq] case class Reservation(messageId: Message.Id, reservationId: Reservation.Id)

/**
 * Internal API.
 */
@InternalApi
private[ironmq] object Reservation {
  case class Id(value: String) extends AnyVal {
    override def toString: String = value
  }
}
