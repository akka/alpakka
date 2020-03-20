/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.Done
import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.stream.alpakka.cassandra.{CassandraSessionSettings, CqlSessionProvider}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config

/**
 * This Cassandra session registry makes it possible to share Cassandra sessions between multiple use sites
 * in the same `ActorSystem` (important for the Cassandra Akka Persistence plugin where it is shared between journal,
 * query plugin and snapshot plugin)
 */
object CassandraSessionRegistry extends ExtensionId[CassandraSessionRegistry] with ExtensionIdProvider {

  def createExtension(system: ExtendedActorSystem): CassandraSessionRegistry =
    new CassandraSessionRegistry(system)

  override def apply(system: ActorSystem): CassandraSessionRegistry = super.apply(system)

  // This is not source compatible with Akka 2.6 as it lacks `overrride`
  def apply(system: ClassicActorSystemProvider): CassandraSessionRegistry =
    apply(system.classicSystem)

  override def lookup(): ExtensionId[CassandraSessionRegistry] = this

  /** Hash key for `sessions`. */
  private case class SessionKey(configPath: String)

  private def sessionKey(settings: CassandraSessionSettings) = SessionKey(settings.configPath)
}

final class CassandraSessionRegistry(system: ExtendedActorSystem) extends Extension {

  import CassandraSessionRegistry._

  private val sessions = new ConcurrentHashMap[SessionKey, CassandraSession]

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(configPath: String): CassandraSession =
    sessionFor(CassandraSessionSettings(configPath))

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * The `init` function will be performed once when the session is created, i.e.
   * if `sessionFor` is called from multiple places with different `init` it will
   * only execute the first.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(configPath: String, init: CqlSession => Future[Done]): CassandraSession =
    sessionFor(CassandraSessionSettings(configPath, init))

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CassandraSession manually instead.
   */
  def sessionFor(settings: CassandraSessionSettings): CassandraSession = {
    sessionFor(settings, system.settings.config.getConfig(settings.configPath))
  }

  /**
   * INTERNAL API: Possibility to initialize the `SessionProvider` with a custom `Config`
   * that is different from the ActorSystem's config section for the `configPath`.
   */
  @InternalStableApi private[akka] def sessionFor(settings: CassandraSessionSettings,
                                                  sessionProviderConfig: Config): CassandraSession = {
    val key = sessionKey(settings)
    sessions.computeIfAbsent(key, _ => startSession(settings, key, sessionProviderConfig))
  }

  private def startSession(settings: CassandraSessionSettings,
                           key: SessionKey,
                           sessionProviderConfig: Config): CassandraSession = {
    val sessionProvider = CqlSessionProvider(system, sessionProviderConfig)
    val log = Logging(system, classOf[CassandraSession])
    val executionContext = system.dispatchers.lookup(sessionProviderConfig.getString("session-dispatcher"))
    new CassandraSession(system,
                         sessionProvider,
                         executionContext,
                         log,
                         metricsCategory = settings.metricsCategory,
                         init = settings.init.getOrElse(_ => Future.successful(Done)),
                         onClose = () => sessions.remove(key))
  }

  /**
   * Closes all registered Cassandra sessions.
   * @param executionContext when used after actor system termination, a different execution context must be provided
   */
  private def close(executionContext: ExecutionContext) = {
    implicit val ec: ExecutionContext = executionContext
    val closing = sessions.values().asScala.map(_.close(ec))
    Future.sequence(closing)
  }

  system.whenTerminated.foreach(_ => close(ExecutionContext.global))(ExecutionContext.global)

}
