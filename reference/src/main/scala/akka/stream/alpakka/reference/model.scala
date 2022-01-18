/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.reference

import java.util.{Optional, OptionalInt}

import akka.annotation.InternalApi
import akka.util.ByteString

import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._
import scala.util.{Success, Try}

/**
 * Use "Read" in message data types to signify that the message was read from outside.
 *
 * The constructor is INTERNAL API, but you may construct instances for testing by using
 * [[akka.stream.alpakka.reference.testkit.MessageFactory]].
 */
final class ReferenceReadResult @InternalApi private[reference] (
    val data: immutable.Seq[ByteString] = immutable.Seq.empty,
    val bytesRead: Try[Int] = Success(0)
) {

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): java.util.List[ByteString] =
    data.asJava

  /**
   * Java API
   *
   * If the model class is scala.util.Try, then two getters should be created.
   * One for getting the value, and another for getting the exception.
   *
   * Return bytes read wrapped in OptionalInt if the Try contains a value,
   * otherwise return empty Optional.
   */
  def getBytesRead(): OptionalInt =
    bytesRead.toOption.asPrimitive

  /**
   * Java API
   *
   * Return the exception wrapped in Optional if the Try contains a Failure,
   * otherwise return empty Optional.
   */
  def getBytesReadFailure(): Optional[Throwable] =
    bytesRead.failed.toOption.asJava

  override def toString: String =
    s"ReferenceReadMessage(data=$data, bytesRead=$bytesRead)"
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
   * create a setter that takes a corresponding Java collection type.
   */
  def withData(data: java.util.List[ByteString]): ReferenceWriteMessage =
    copy(data = data.asScala.toIndexedSeq)

  /**
   * Java API
   *
   * When settings class has an attribute of Scala Long class,
   * Java setter needs to take Java Long class and convert to Scala Long.
   */
  def withMetrics(metrics: java.util.Map[String, java.lang.Long]): ReferenceWriteMessage =
    copy(metrics = metrics.asScala.view.mapValues(Long.unbox).toMap)

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): java.util.List[ByteString] =
    data.asJava

  /**
   * Java API
   *
   * Java getter needs to return Java Long classes which is converted from Scala Long.
   */
  def getMetrics(): java.util.Map[String, java.lang.Long] =
    metrics.map {
      case (key, value) => key -> java.lang.Long.valueOf(value)
    }.asJava

  private def copy(data: immutable.Seq[ByteString] = data, metrics: Map[String, Long] = metrics) =
    new ReferenceWriteMessage(data, metrics)

  override def toString: String =
    s"ReferenceWriteMessage(data=$data, metrics=$metrics)"
}

object ReferenceWriteMessage {
  def apply(): ReferenceWriteMessage = new ReferenceWriteMessage()

  def create(): ReferenceWriteMessage = ReferenceWriteMessage()
}

/**
 * The result returned by the flow for each [[ReferenceWriteMessage]].
 *
 * As this class is not meant to be instantiated outside of this connector
 * the constructor is marked as INTERNAL API.
 *
 * The constructor is INTERNAL API, but you may construct instances for testing by using
 * [[akka.stream.alpakka.reference.testkit.MessageFactory]].
 */
final class ReferenceWriteResult @InternalApi private[reference] (val message: ReferenceWriteMessage,
                                                                  val metrics: Map[String, Long],
                                                                  val status: Int) {

  /** Java API */
  def getMessage: ReferenceWriteMessage = message

  /**
   * Java API
   *
   * Java getter needs to return Java Long classes which is converted from Scala Long.
   */
  def getMetrics(): java.util.Map[String, java.lang.Long] =
    metrics.map {
      case (key, value) => key -> java.lang.Long.valueOf(value)
    }.asJava

  /** Java API */
  def getStatus: Int = status

  override def toString =
    s"""ReferenceWriteResult(message=$message,status=$status)"""

  override def equals(other: Any): Boolean = other match {
    case that: ReferenceWriteResult =>
      java.util.Objects.equals(this.message, that.message) &&
      java.util.Objects.equals(this.status, that.status)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(message, Int.box(status))
}
