/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.util.Optional
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object CouchbaseWriteSettings {

  // TODO name it `noReplication` or YOLO?
  val default = CouchbaseWriteSettings(1, ReplicateTo.ONE, PersistTo.NONE, 2.seconds)

  def apply(): CouchbaseWriteSettings = default

  def apply(parallelism: Int,
            replicateTo: ReplicateTo,
            persistTo: PersistTo,
            timeout: FiniteDuration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout)

  def create(): CouchbaseWriteSettings = default

  def create(parallelism: Int,
             replicateTo: ReplicateTo,
             persistTo: PersistTo,
             timeout: java.time.Duration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism,
                               replicateTo,
                               persistTo,
                               FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS))

}

final class CouchbaseWriteSettings private (val parallelism: Int,
                                            val replicateTo: ReplicateTo,
                                            val persistTo: PersistTo,
                                            val timeout: FiniteDuration) {

  def withParallelism(parallelism: Int): CouchbaseWriteSettings = copy(parallelism = parallelism)

  def withReplicateTo(replicateTo: ReplicateTo): CouchbaseWriteSettings = copy(replicateTo = replicateTo)

  def withPersistTo(persistTo: PersistTo): CouchbaseWriteSettings = copy(persistTo = persistTo)

  /**
   * Java API:
   */
  def withTimeout(timeout: java.time.Duration): CouchbaseWriteSettings =
    copy(timeout = FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS))

  /**
   * Scala API:
   */
  def withTimeout(timeout: FiniteDuration): CouchbaseWriteSettings = copy(timeout = timeout)

  private[this] def copy(parallelism: Int = parallelism,
                         replicateTo: ReplicateTo = replicateTo,
                         persistTo: PersistTo = persistTo,
                         timeout: FiniteDuration = timeout) =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout)

  override def toString: String =
    "CouchbaseWriteSettings(" +
    s"parallelism=$parallelism," +
    s"replicateTo=$replicateTo," +
    s"persistTo=$persistTo," +
    s"timeout=${timeout.toCoarsest}" +
    ")"
}

object CouchbaseSessionSettings {

  val configPath = "alpakka.couchbase.session"

  /**
   * Scala API:
   * Load the session from the given config object, expects the config object to have the fields `username`,
   * `password` and `nodes`. Using it means first looking your config namespace up yourself using `config.getConfig("some.path")`.
   */
  def apply(config: Config): CouchbaseSessionSettings = {
    val username = config.getString("username")
    val password = config.getString("password")
    val nodes = config.getStringList("nodes").asScala.toList
    new CouchbaseSessionSettings(username, password, nodes, None)
  }

  /**
   * Scala API:
   * Load the session from the default config path `alpakka.couchbase.session`, expects the config object to have the fields `username`,
   * `password` and `nodes`.
   */
  def apply(system: ActorSystem): CouchbaseSessionSettings =
    apply(system.settings.config.getConfig(configPath))

  /**
   * Scala API:
   */
  def apply(username: String, password: String): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, Nil, None)

  /**
   * Java API:
   */
  def create(username: String, password: String): CouchbaseSessionSettings =
    apply(username, password)

  /**
   * Java API:
   * Load the session from the given config object, expects the config object to have the fields `username`,
   * `password` and `nodes`. Using it means first looking your config namespace up yourself using `config.getConfig("some.path")`.
   */
  def create(config: Config): CouchbaseSessionSettings = apply(config)

  /**
   * Java API:
   * Load the session from the default config path `alpakka.couchbase.session`, expects the config object to have the fields `username`,
   * `password` and `nodes`.
   */
  def create(system: ActorSystem): CouchbaseSessionSettings =
    apply(system.settings.config.getConfig(configPath))

}

final class CouchbaseSessionSettings private (val username: String,
                                              val password: String,
                                              val nodes: immutable.Seq[String],
                                              val environment: Option[CouchbaseEnvironment]) {

  def withUsername(username: String): CouchbaseSessionSettings =
    copy(username = username)

  def withPassword(password: String): CouchbaseSessionSettings =
    copy(password = password)

  def withNodes(nodes: String): CouchbaseSessionSettings =
    copy(nodes = nodes :: Nil)

  def withNodes(nodes: immutable.Seq[String]): CouchbaseSessionSettings =
    copy(nodes = nodes)

  /** Java API */
  def withNodes(nodes: java.util.List[String]): CouchbaseSessionSettings =
    copy(nodes = nodes.asScala.toList)

  def withEnvironment(environment: CouchbaseEnvironment): CouchbaseSessionSettings =
    copy(environment = Some(environment))

  private def copy(username: String = username,
                   password: String = password,
                   nodes: immutable.Seq[String] = nodes,
                   environment: Option[CouchbaseEnvironment] = environment): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, nodes, environment)

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseSessionSettings =>
      username == that.username &&
      password == that.password &&
      nodes == that.nodes &&
      environment == that.environment
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(username, password, nodes, environment)

  override def toString =
    "CouchbaseSessionSettings(" +
    s"username=$username," +
    s"password=*****," +
    s"nodes=${nodes.mkString("[", ", ", "]")}," +
    s"environment=$environment" +
    ")"
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
