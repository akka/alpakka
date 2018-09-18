/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

object JmsProducerMessage {

  sealed trait Envelope[+M <: JmsMessage, +PassThrough] {
    def passThrough: PassThrough
  }

  def message[M <: JmsMessage, PassThrough](message: M, passThrough: PassThrough): Envelope[M, PassThrough] =
    Message(message, passThrough)

  def passThroughMessage[M <: JmsMessage, PassThrough](passThrough: PassThrough): Envelope[M, PassThrough] =
    PassThroughMessage(passThrough)

  final case class Message[+M <: JmsMessage, +PassThrough](message: M, passThrough: PassThrough)
      extends Envelope[M, PassThrough]

  final case class PassThroughMessage[+M <: JmsMessage, +PassThrough](passThrough: PassThrough)
      extends Envelope[M, PassThrough]
}
