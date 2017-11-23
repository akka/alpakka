/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl

import akka.NotUsed

import akka.stream.alpakka.jms.{JmsMessage, JmsSinkSettings, JmsSinkStage}
import akka.stream.scaladsl.Flow

import scala.collection.JavaConversions

object JmsSink {

  /**
   * Java API: Creates an [[JmsSink]] for [[JmsMessage]]s
   */
  def create[R <: JmsMessage](jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[R, NotUsed] =
    akka.stream.javadsl.Sink.fromGraph(new JmsSinkStage(jmsSinkSettings))

  /**
   * Java API: Creates an [[JmsSink]] for strings
   */
  def textSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[String, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSink.textSink(jmsSinkSettings).asJava

  /**
   * Java API: Creates an [[JmsSink]] for bytes
   */
  def bytesSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[Array[Byte], NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSink.bytesSink(jmsSinkSettings).asJava

  /**
   * Java API: Creates an [[JmsSink]] for maps with primitive datatypes as value
   */
  def mapSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[java.util.Map[String, Any], NotUsed] = {

    val scalaSink = akka.stream.alpakka.jms.scaladsl.JmsSink.mapSink(jmsSinkSettings)
    val javaToScalaConversion =
      Flow.fromFunction((javaMap: java.util.Map[String, Any]) => JavaConversions.mapAsScalaMap(javaMap).toMap)
    javaToScalaConversion.to(scalaSink).asJava
  }

  /**
   * Java API: Creates an [[JmsSink]] for serializable objects
   */
  def objectSink(jmsSinkSettings: JmsSinkSettings): akka.stream.javadsl.Sink[java.io.Serializable, NotUsed] =
    akka.stream.alpakka.jms.scaladsl.JmsSink.objectSink(jmsSinkSettings).asJava

}
