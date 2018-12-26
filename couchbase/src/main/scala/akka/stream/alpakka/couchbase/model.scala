/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.util.Optional

import akka.annotation.InternalApi
import com.couchbase.client.java.{PersistTo, ReplicateTo}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

final class CouchbaseWriteSettings private (val parallelism: Int,
                                            val replicateTo: ReplicateTo,
                                            val persistTo: PersistTo,
                                            val timeout: Long,
                                            val timeUnit: java.util.concurrent.TimeUnit) {

  def withParallelism(parallelism: Int): CouchbaseWriteSettings = copy(parallelism = parallelism)

  def withReplicateTo(replicateTo: ReplicateTo): CouchbaseWriteSettings = copy(replicateTo = replicateTo)

  def withPersistTo(persistTo: PersistTo): CouchbaseWriteSettings = copy(persistTo = persistTo)

  def withTimeOut(timeout: Long, timeUnit: java.util.concurrent.TimeUnit): CouchbaseWriteSettings =
    copy(timeout = timeout, timeUnit = timeUnit)

  private[this] def copy(parallelism: Int = parallelism,
                         replicateTo: ReplicateTo = replicateTo,
                         persistTo: PersistTo = persistTo,
                         timeout: Long = timeout,
                         timeUnit: java.util.concurrent.TimeUnit = timeUnit) =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout, timeUnit)

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseWriteSettings =>
      parallelism == that.parallelism &&
      replicateTo == that.replicateTo &&
      persistTo == that.persistTo &&
      timeout == that.timeout &&
      timeUnit == that.timeUnit
    case _ => false
  }

  override def hashCode(): Int = {
    31 * parallelism.hashCode() + 31 * replicateTo.hashCode() + 31 * persistTo.hashCode()
    +31 * timeout.hashCode() + 31 * timeUnit.hashCode()
  }
}

object CouchbaseWriteSettings {

  def apply(parallelism: Int = 1,
            replicateTo: ReplicateTo = ReplicateTo.ONE,
            persistTo: PersistTo = PersistTo.NONE,
            timeout: Long = 2L,
            timeUnit: java.util.concurrent.TimeUnit = java.util.concurrent.TimeUnit.SECONDS): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout, timeUnit)

  def create(): CouchbaseWriteSettings = CouchbaseWriteSettings()

}

final class FailedOperation private (val id: String, val ex: Throwable) {

  override def equals(other: Any): Boolean = other match {
    case that: FailedOperation =>
      id == that.id &&
      ex == that.ex
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, ex)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object FailedOperation {

  /** Scala API */
  @InternalApi
  private[couchbase] def apply(id: String, ex: Throwable) = new FailedOperation(id = id, ex = ex)

  /** Java API */
  @InternalApi
  private[couchbase] def create(id: String, ex: Throwable) = new FailedOperation(id = id, ex = ex)
}

final case class SingleOperationResult[T](entity: T, result: Try[String]) {

  /** Java API */
  def getEntity: T = entity

  /** Java API */
  def getException: Optional[Throwable] =
    result match {
      case Failure(ex) => Optional.of(ex)
      case Success(_) => Optional.empty()

    }
}

final case class BulkOperationResult[T](entities: Seq[T], failures: Seq[FailedOperation] = Seq[FailedOperation]()) {

  /** Java API */
  def getEntities: java.util.List[T] = entities.asJava

  /** Java API */
  def getFailures: java.util.List[FailedOperation] = failures.asJava
}
