/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package jms

import javax.jms.ConnectionFactory

import akka.stream.alpakka.jms.JmsSinkSettings
import akka.stream.alpakka.jms.scaladsl.JmsSink
import akka.stream.scaladsl.{Sink, Source}
import playground.ActorSystemAvailable

class JmsSampleBase extends ActorSystemAvailable {

  def enqueue(connectionFactory: ConnectionFactory)(msgs: String*): Unit = {
    val jmsSink: Sink[String, _] =
      JmsSink.textSink(
        JmsSinkSettings(connectionFactory).withQueue("test")
      )
    Source(msgs.toList).runWith(jmsSink)
  }
}
