/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.stream.alpakka.ironmq.Message
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

/**
 * Internal API.
 *
 * Simplified representation of the IronMq queue for JSON conversion.
 *
 * @param name The name associated with this Queue.
 */
private[ironmq] case class Queue(name: Queue.Name)

private[ironmq] object Queue {

  case class Name(value: String) extends AnyVal {
    override def toString: String = value
  }
}

private[ironmq] trait Codec {

  implicit val messageIdEncoder: Encoder[Message.Id] = Encoder.instance { id =>
    Json.fromString(id.value)
  }

  implicit val messageIdDecoder: Decoder[Message.Id] = Decoder.instance { cursor =>
    cursor.as[String].map(Message.Id.apply)
  }

  implicit val reservationIdEncoder: Encoder[Reservation.Id] = Encoder.instance { id =>
    Json.fromString(id.value)
  }

  implicit val reservationIdDecoder: Decoder[Reservation.Id] = Decoder.instance { cursor =>
    cursor.as[String].map(Reservation.Id.apply)
  }

  implicit val messageIdsDecoder: Decoder[Message.Ids] = Decoder.instance { cursor =>
    cursor.downField("ids").as[List[Message.Id]].map(Message.Ids.apply)
  }

  implicit val queueDecoder: Decoder[Queue] = Decoder.instance { cursor =>
    for {
      name <- cursor.downField("name").as[Queue.Name]
    } yield Queue(name)
  }

  implicit val queueNameDecoder: Decoder[Queue.Name] = Decoder.instance { cursor =>
    cursor.as[String].map(Queue.Name.apply)
  }

  implicit val queueNameEncoder: Encoder[Queue.Name] = Encoder.instance { qn =>
    Json.fromString(qn.value)
  }

  implicit val messageDecoder: Decoder[Message] = Decoder.instance { cursor =>
    for {
      id <- cursor.downField("id").as[Message.Id]
      body <- cursor.downField("body").as[String]
      noOfReservations <- cursor.downField("reserved_count").as[Int]
    } yield Message(id, body, noOfReservations)
  }

  implicit val reservedMessageDecoder: Decoder[ReservedMessage] = Decoder.instance { cursor =>
    for {
      message <- cursor.as[Message]
      reservationId <- cursor.downField("reservation_id").as[Reservation.Id]
    } yield ReservedMessage(reservationId, message)
  }

  implicit val reservationEncoder: Encoder[Reservation] = Encoder.instance { r =>
    Json.obj("id" -> r.messageId.asJson, "reservation_id" -> r.reservationId.asJson)
  }
}

private[ironmq] object Codec extends Codec
