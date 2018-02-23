/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

import akka.Done
import akka.stream.alpakka.jms.JmsProducerSettings
import akka.stream.alpakka.jms.scaladsl.JmsProducer
import akka.stream.scaladsl.{Sink, Source}
import javax.jms.ConnectionFactory
import playground.ActorSystemAvailable

import scala.concurrent.Future

class JmsSampleBase extends ActorSystemAvailable {

  def enqueue(connectionFactory: ConnectionFactory)(msgs: String*): Unit = {
    val jmsSink: Sink[String, Future[Done]] =
      JmsProducer.textSink(
        JmsProducerSettings(connectionFactory).withQueue("test")
      )
    Source(msgs.toList).runWith(jmsSink)
  }
}
