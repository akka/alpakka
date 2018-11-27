/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.streams.alpakka.redis

import java.util.Optional

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}

final class RedisKeyValue[K, V] private (val key: K, val value: V) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisKeyValue[K, V] =>
      key == that.key &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(key, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

final class RedisKeyValues[K, V] private (val key: K, val values: Array[V]) {
  override def equals(other: Any): Boolean = other match {
    case that: RedisKeyValues[K, V] =>
      key == that.key &&
      (values sameElements that.values)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(key, values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

final case class RedisPSubscribeResult[K, V](pattern: K, key: K, value: V) {}

final class RedisPubSub[K, V] private (val channel: K, val value: V) {

  override def equals(other: Any): Boolean = other match {
    case that: RedisPubSub[_, _] =>
      channel == that.channel &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(channel, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object RedisPubSub {

  def apply[K, V](topic: K, value: V): RedisPubSub[K, V] = new RedisPubSub(topic, value)
  def create[K, V](topic: K, value: V): RedisPubSub[K, V] = new RedisPubSub(topic, value)
}

object RedisKeyValues {

  def apply[K, V](key: K, values: Array[V]) = new RedisKeyValues(key, values)
  def create[K, V](key: K, values: Array[V]) = new RedisKeyValues(key, values)
}

object RedisKeyValue {

  def apply[K, V](key: K, value: V) = new RedisKeyValue(key, value)
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
/*
object FailedOperation {

  /** Scala API */
  @InternalApi
  private[redis] def apply(id: String, ex: Throwable) = new FailedOperation(key = id, ex = ex)

  /** Java API */
  @InternalApi
  private[redis] def create(id: String, ex: Throwable) = new FailedOperation(key = id, ex = ex)
}

final case class BulkOperationResult[T](entities: Seq[T], failures: Seq[FailedOperation] = Seq[FailedOperation]()) {

  /** Java API */
  def getEntities: java.util.List[T] = entities.asJava

  /** Java API */
  def getFailures: java.util.List[FailedOperation] = failures.asJava
}
 */
