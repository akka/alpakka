/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.jms.{JmsMessage, JmsProducerSettings}

@deprecated("Use JmsProducer instead", "0.18")
object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def create[R <: JmsMessage](
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[R, CompletionStage[Done]] =
    JmsProducer.create(jmsSinkSettings)

  /**
   * Java API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSinkSettings: JmsProducerSettings): akka.stream.javadsl.Sink[String, CompletionStage[Done]] =
    JmsProducer.textSink(jmsSinkSettings)

  /**
   * Java API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSinkSettings: JmsProducerSettings): akka.stream.javadsl.Sink[Array[Byte], CompletionStage[Done]] =
    JmsProducer.bytesSink(jmsSinkSettings)

  /**
   * Java API: Creates an [[JmsSink]] for maps with primitive datatypes as value
   */
  def mapSink(
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.util.Map[String, Any], CompletionStage[Done]] =
    JmsProducer.mapSink(jmsSinkSettings)

  /**
   * Java API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(
      jmsSinkSettings: JmsProducerSettings
  ): akka.stream.javadsl.Sink[java.io.Serializable, CompletionStage[Done]] =
    JmsProducer.objectSink(jmsSinkSettings)

}
