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
import scala.collection.JavaConverters._
import akka.japi.Pair

import scala.util.{Failure, Success, Try}

/**
 * Use "Read" in message data types to signify that the message was read from outside.
 */
final class ReferenceReadMessage private (
    val data: immutable.Seq[ByteString] = immutable.Seq.empty,
    val bytesRead: Try[Int] = Success(0)
) {
  def withData(data: immutable.Seq[ByteString]): ReferenceReadMessage =
    copy(data = data)

  def withBytesRead(bytesRead: Try[Int]): ReferenceReadMessage =
    copy(bytesRead = bytesRead)

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): JavaList[ByteString] =
    data.asJava

  /**
   * Java API
   *
   * If the model class is scala.util.Try, then two getters should be created.
   * One for getting the value, and another for getting the exception.
   *
   * Should return null if the Try contains a Failure.
   */
  def getBytesRead(): Integer = bytesRead match {
    case Success(bytesRead) => bytesRead
    case Failure(_) => null
  }

  /**
   * Java API
   *
   * Return the exception if the Try contains a Failure, otherwise return a null.
   */
  def getBytesReadFailure(): Throwable = bytesRead match {
    case Success(_) => null
    case Failure(ex) => ex
  }

  private def copy(data: immutable.Seq[ByteString] = data, bytesRead: Try[Int] = bytesRead) =
    new ReferenceReadMessage(data, bytesRead)

  override def toString: String =
    s"ReferenceReadMessage(data=$data, bytesRead=$bytesRead)"
}

object ReferenceReadMessage {
  def apply(): ReferenceReadMessage = new ReferenceReadMessage()

  def create(): ReferenceReadMessage = ReferenceReadMessage()
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
  def withData(data: JavaList[ByteString]): ReferenceWriteMessage =
    copy(data = data.asScala.toIndexedSeq)

  /**
   * Java API
   *
   * When settings class has an attribute of Scala Long class,
   * Java setter needs to take Java Long class and convert to Scala Long.
   */
  def withMetrics(metrics: JavaMap[String, JavaLong]): ReferenceWriteMessage =
    copy(metrics = metrics.asScala.mapValues(Long.unbox).toMap)

  /**
   * Java API
   *
   * If the model class is meant to be also consumed from the user API,
   * but the attribute class is Scala specific, create getter for Java API.
   */
  def getData(): JavaList[ByteString] =
    data.asJava

  /**
   * Java API
   *
   * Java getter needs to return Java Long classes which is converted from Scala Long.
   */
  def getMetrics(): JavaMap[String, JavaLong] =
    metrics.mapValues(JavaLong.valueOf).asJava

  private def copy(data: immutable.Seq[ByteString] = data, metrics: Map[String, Long] = metrics) =
    new ReferenceWriteMessage(data, metrics)

  override def toString: String =
    s"ReferenceWriteMessage(data=$data, metrics=$metrics)"
}

object ReferenceWriteMessage {
  def apply(): ReferenceWriteMessage = new ReferenceWriteMessage()

  def create(): ReferenceWriteMessage = ReferenceWriteMessage()
}
