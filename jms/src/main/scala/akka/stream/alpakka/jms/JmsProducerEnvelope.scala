/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

sealed trait JmsProducerEnvelope[+M <: JmsMessage, +PassThrough] {
  def passThrough: PassThrough
}

object JmsProducerEnvelope {

  def message[M <: JmsMessage, PassThrough](message: M, passThrough: PassThrough): JmsProducerEnvelope[M, PassThrough] =
    Message(message, passThrough)

  def passThroughMessage[M <: JmsMessage, PassThrough](passThrough: PassThrough): JmsProducerEnvelope[M, PassThrough] =
    PassThroughMessage(passThrough)

  final case class Message[+M <: JmsMessage, +PassThrough](message: M, passThrough: PassThrough)
      extends JmsProducerEnvelope[M, PassThrough]

  final case class PassThroughMessage[+M <: JmsMessage, +PassThrough](passThrough: PassThrough)
      extends JmsProducerEnvelope[M, PassThrough]
}
