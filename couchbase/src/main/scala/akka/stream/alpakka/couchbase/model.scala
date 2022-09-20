/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.util.concurrent.{CompletionStage, TimeUnit}

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.annotation.InternalApi
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._

/**
 * Configure Couchbase writes.
 */
object CouchbaseWriteSettings {

  /**
   * Simple settings not requiring replication nor persistence.
   */
  val inMemory = CouchbaseWriteSettings(1, ReplicateTo.NONE, PersistTo.NONE, 2.seconds)

  def apply(): CouchbaseWriteSettings = inMemory

  def apply(parallelism: Int,
            replicateTo: ReplicateTo,
            persistTo: PersistTo,
            timeout: FiniteDuration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout)

  def create(): CouchbaseWriteSettings = inMemory

  def create(parallelism: Int,
             replicateTo: ReplicateTo,
             persistTo: PersistTo,
             timeout: java.time.Duration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism,
                               replicateTo,
                               persistTo,
                               FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS))

}

/**
 * Configure Couchbase writes.
 */
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

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseWriteSettings =>
      this.parallelism == that.parallelism &&
      this.replicateTo == that.replicateTo &&
      this.persistTo == that.persistTo &&
      this.timeout == that.timeout
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(int2Integer(parallelism), replicateTo, persistTo, timeout)

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
    new CouchbaseSessionSettings(username, password, nodes, environment = None, enrichAsync = Future.successful)
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
   * Load the session from the default config path `alpakka.couchbase.session`, expects the config object to have the fields `username`,
   * `password` and `nodes`.
   */
  def apply(system: ClassicActorSystemProvider): CouchbaseSessionSettings =
    apply(system.classicSystem)

  /**
   * Scala API:
   */
  def apply(username: String, password: String): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, Nil, environment = None, enrichAsync = Future.successful)

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

  /**
   * Java API:
   * Load the session from the default config path `alpakka.couchbase.session`, expects the config object to have the fields `username`,
   * `password` and `nodes`.
   */
  def create(system: ClassicActorSystemProvider): CouchbaseSessionSettings =
    apply(system.classicSystem)
}

final class CouchbaseSessionSettings private (
    val username: String,
    val password: String,
    val nodes: immutable.Seq[String],
    val environment: Option[CouchbaseEnvironment],
    val enrichAsync: CouchbaseSessionSettings => Future[CouchbaseSessionSettings]
) {

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

  /** Scala API:
   * Allows to provide an asynchronous method to update the settings.
   */
  def withEnrichAsync(value: CouchbaseSessionSettings => Future[CouchbaseSessionSettings]): CouchbaseSessionSettings =
    copy(enrichAsync = value)

  /** Java API:
   * Allows to provide an asynchronous method to update the settings.
   */
  def withEnrichAsyncCs(
      value: java.util.function.Function[CouchbaseSessionSettings, CompletionStage[CouchbaseSessionSettings]]
  ): CouchbaseSessionSettings =
    copy(enrichAsync = (s: CouchbaseSessionSettings) => value.apply(s).toScala)

  def withEnvironment(environment: CouchbaseEnvironment): CouchbaseSessionSettings =
    copy(environment = Some(environment))

  /**
   * Internal API.
   * Used internally to apply the asynchronous settings enrichment function.
   */
  @InternalApi
  def enriched: Future[CouchbaseSessionSettings] = enrichAsync(this)

  private def copy(
      username: String = username,
      password: String = password,
      nodes: immutable.Seq[String] = nodes,
      environment: Option[CouchbaseEnvironment] = environment,
      enrichAsync: CouchbaseSessionSettings => Future[CouchbaseSessionSettings] = enrichAsync
  ): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, nodes, environment, enrichAsync)

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

  override def toString: String =
    "CouchbaseSessionSettings(" +
    s"username=$username," +
    s"password=*****," +
    s"nodes=${nodes.mkString("[", ", ", "]")}," +
    s"environment=$environment" +
    ")"
}

/**
 * Wrapper to for handling Couchbase write failures in-stream instead of failing the stream.
 */
sealed trait CouchbaseWriteResult[T <: Document[_]] {
  def isSuccess: Boolean
  def isFailure: Boolean
  def doc: T
}

/**
 * Emitted for a successful Couchbase write operation.
 */
final case class CouchbaseWriteSuccess[T <: Document[_]] private (override val doc: T) extends CouchbaseWriteResult[T] {
  val isSuccess: Boolean = true
  val isFailure: Boolean = false
}

/**
 * Emitted for a failed Couchbase write operation.
 */
final case class CouchbaseWriteFailure[T <: Document[_]] private (override val doc: T, failure: Throwable)
    extends CouchbaseWriteResult[T] {
  val isSuccess: Boolean = false
  val isFailure: Boolean = true
}

/**
 * Wrapper to for handling Couchbase write failures in-stream instead of failing the stream.
 */
sealed trait CouchbaseDeleteResult {
  def isSuccess: Boolean
  def isFailure: Boolean
  def id: String
}

/**
 * Emitted for a successful Couchbase write operation.
 */
final case class CouchbaseDeleteSuccess private (override val id: String) extends CouchbaseDeleteResult {
  val isSuccess: Boolean = true
  val isFailure: Boolean = false
}

/**
 * Emitted for a failed Couchbase write operation.
 */
final case class CouchbaseDeleteFailure private (override val id: String, failure: Throwable)
    extends CouchbaseDeleteResult {
  val isSuccess: Boolean = false
  val isFailure: Boolean = true
}
