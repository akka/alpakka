/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis

import java.util.Optional

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

final class RedisHMSet[K, V] private (val key: K, val values: Seq[RedisKeyValue[K, V]]) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisHMSet[K, V] =>
      key == that.key &&
      values.length == that.values.length &&
      values.zip(that.values).forall { case (a, b) => a == b }
    case _ => false
  }

  override def hashCode(): Int =
    values.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b) + 31 * key.hashCode()
}

object RedisHMSet {

  def create[K, V](key: K, values: java.util.List[RedisKeyValue[K, V]]): RedisHMSet[K, V] =
    new RedisHMSet[K, V](key, values.asScala.to[Seq])
  def apply[K, V](key: K, values: Seq[RedisKeyValue[K, V]]): RedisHMSet[K, V] = new RedisHMSet[K, V](key, values)
}

final class RedisHSet[K, V] private (val key: K, val field: K, val value: V) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisHSet[K, V] =>
      key == that.key &&
      field == that.field &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int =
    31 * key.hashCode() + 31 * field.hashCode() + 31 * value.hashCode()
}

object RedisHSet {

  def apply[K, V](key: K, field: K, value: V): RedisHSet[K, V] = new RedisHSet(key, field, value)
  def create[K, V](key: K, field: K, value: V): RedisHSet[K, V] = new RedisHSet(key, field, value)
}

final class RedisKeyValue[K, V] private (val key: K, val value: V) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisKeyValue[K, V] =>
      key == that.key &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int =
    31 * key.hashCode() + 31 * value.hashCode()
}

final class RedisHKeyFields[K] private (val key: K, val fields: Seq[K]) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisHKeyFields[K] =>
      key == that.key &&
      fields == that.fields
    case _ => false
  }

  override def hashCode(): Int =
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b) + 31 + key.hashCode()

}

object RedisHKeyFields {
  def apply[K](key: K, fields: Seq[K]): RedisHKeyFields[K] = new RedisHKeyFields(key, fields)

  def create[K](key: K, fields: java.util.List[K]): RedisHKeyFields[K] =
    new RedisHKeyFields(key, fields.asScala.to[Seq])
}

final class RedisKeyValues[K, V] private (val key: K, val values: Array[V]) {
  override def equals(other: Any): Boolean = other match {
    case that: RedisKeyValues[K, V] =>
      key == that.key &&
      (values sameElements that.values)
    case _ => false
  }

  override def hashCode(): Int =
    values.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b) + 31 * key.hashCode()
}

final case class RedisPSubscribeResult[K, V](pattern: K, key: K, value: V) {}

final class RedisPubSub[K, V] private (val channel: K, val value: V) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisPubSub[_, _] =>
      channel == that.channel &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int =
    31 * channel.hashCode() + 31 * value.hashCode()
}

object RedisPubSub {

  def apply[K, V](topic: K, value: V): RedisPubSub[K, V] = new RedisPubSub(topic, value)
  def create[K, V](topic: K, value: V): RedisPubSub[K, V] = new RedisPubSub(topic, value)
}

object RedisKeyValues {

  def apply[K, V](key: K, values: Array[V]): RedisKeyValues[K, V] = new RedisKeyValues(key, values)
  def create[K, V](key: K, values: Array[V]): RedisKeyValues[K, V] = new RedisKeyValues(key, values)
}

object RedisKeyValue {

  def apply[K, V](key: K, value: V): RedisKeyValue[K, V] = new RedisKeyValue(key, value)
  def create[K, V](key: K, value: V) = new RedisKeyValue(key, value)
}

final case class RedisOperationResult[T, R] private (output: T, result: Try[R]) {

  /** Java API */
  def getOutput: T = output

  /** Java API */
  def getException: Optional[Throwable] =
    result match {
      case Failure(ex) => Optional.of(ex)
      case Success(_) => Optional.empty()
    }
}

final class RedisSubscriberSettings private (val maxConcurrency: Int,
                                             val maxBufferSize: Int,
                                             val unsubscribeOnShutDown: Boolean) {

  def withMaxConcurrency(maxConcurrency: Int): RedisSubscriberSettings = copy(maxConcurrency = maxConcurrency)

  def withMaxBufferSize(maxBufferSize: Int): RedisSubscriberSettings = copy(maxBufferSize = maxBufferSize)

  def withUnsubscribeOnShutDown(unsubscribeOnShutDown: Boolean): RedisSubscriberSettings =
    copy(unsubscribeOnShutDown = unsubscribeOnShutDown)

  private[this] def copy(maxConcurrency: Int = maxConcurrency,
                         maxBufferSize: Int = maxBufferSize,
                         unsubscribeOnShutDown: Boolean = unsubscribeOnShutDown) =
    new RedisSubscriberSettings(maxConcurrency, maxBufferSize, unsubscribeOnShutDown)
}

object RedisSubscriberSettings {
  val Defaults = new RedisSubscriberSettings(100, 1000, true)

  /**
   * Scala API
   */
  def apply(): RedisSubscriberSettings = Defaults

  /**
   * Java API
   */
  def create(): RedisSubscriberSettings = Defaults
}
