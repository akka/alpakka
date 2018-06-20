/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference

// rename Java imports if the name clashes with the Scala name
import java.lang.{Long => JavaLong}
import java.util.{List => JavaList, Map => JavaMap}

import akka.util.ByteString

import scala.annotation.varargs
import scala.collection.immutable

import akka.japi.Pair

/**
 * Use "Read" in message data types to signify that the message was read from outside.
 */
final class ReferenceReadMessage private (
    val data: immutable.Seq[ByteString] = immutable.Seq.empty
) {
  def withData(data: immutable.Seq[ByteString]): ReferenceReadMessage =
    copy(data = data)

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): JavaList[ByteString] = {
    import scala.collection.JavaConverters._
    data.asJava
  }

  private def copy(data: immutable.Seq[ByteString] = data) =
    new ReferenceReadMessage(data)
}

object ReferenceReadMessage {
  def apply(): ReferenceReadMessage = new ReferenceReadMessage()
}

/**
 * Use "Write" in message data types to signify that the messages is to be written to outside.
 */
final class ReferenceWriteMessage private (
    val data: immutable.Seq[ByteString] = immutable.Seq.empty,
    val metrics: Map[String, Long] = Map.empty
) {
  def withData(data: immutable.Seq[ByteString]): ReferenceWriteMessage =
    copy(data = data)

  def withMetrics(metrics: Map[String, Long]): ReferenceWriteMessage =
    copy(metrics = metrics)

  /**
   * Java API
   *
   * When settings class has an attribute of Scala collection type,
   * use varargs annotation to generate a Java API varargs method.
   */
  @varargs def withData(data: ByteString*): ReferenceWriteMessage =
    copy(data = data.to[immutable.Seq])

  /**
   * Java API
   *
   * Java setter needs to take Java Long class and convert to Scala Long.
   */
  @varargs def withMetrics(metrics: Pair[String, JavaLong]*): ReferenceWriteMessage =
    copy(metrics = metrics.map(pair => (pair.first, Long.unbox(pair.second))).toMap)

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): JavaList[ByteString] = {
    import scala.collection.JavaConverters._
    data.asJava
  }

  /**
   * Java getter needs to return Java Long classes which is converted from Scala Long.
   */
  def getMetrics(): JavaMap[String, JavaLong] = {
    import scala.collection.JavaConverters._
    metrics.mapValues(JavaLong.valueOf).asJava
  }

  private def copy(data: immutable.Seq[ByteString] = data, metrics: Map[String, Long] = metrics) =
    new ReferenceWriteMessage(data, metrics)

  override def toString: String =
    s"""ReferenceWriteMessage(
       |  data       = $data
       |  metrics    = $metrics
       |)""".stripMargin
}

object ReferenceWriteMessage {

  def apply(): ReferenceWriteMessage = new ReferenceWriteMessage()

  def create(): ReferenceWriteMessage = ReferenceWriteMessage()
}
