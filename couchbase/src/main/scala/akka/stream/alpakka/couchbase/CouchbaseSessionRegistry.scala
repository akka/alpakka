/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.alpakka.couchbase.impl.CouchbaseClusterRegistry
import akka.stream.alpakka.couchbase.javadsl.{CouchbaseSession => JCouchbaseSession}
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.jdk.FutureConverters._

/**
 * This Couchbase session registry makes it possible to share Couchbase sessions between multiple use sites
 * in the same `ActorSystem` (important for the Couchbase Akka Persistence plugin where it is shared between journal,
 * query plugin and snapshot plugin)
 */
object CouchbaseSessionRegistry extends ExtensionId[CouchbaseSessionRegistry] with ExtensionIdProvider {
  def createExtension(system: ExtendedActorSystem): CouchbaseSessionRegistry =
    new CouchbaseSessionRegistry(system)

  /**
   * Java API: Get the session registry with new actors API.
   */
  override def get(system: ClassicActorSystemProvider): CouchbaseSessionRegistry =
    super.apply(system)

  /**
   * Java API: Get the session registry with the classic actors API.
   */
  override def get(system: akka.actor.ActorSystem): CouchbaseSessionRegistry =
    super.apply(system)

  override def lookup: ExtensionId[CouchbaseSessionRegistry] = this

  private case class SessionKey(settings: CouchbaseSessionSettings, bucketName: String)
}

final class CouchbaseSessionRegistry(system: ExtendedActorSystem) extends Extension {

  import CouchbaseSessionRegistry._

  private val blockingDispatcher = system.dispatchers.lookup("akka.actor.default-blocking-io-dispatcher")

  private val clusterRegistry = new CouchbaseClusterRegistry(system)

  private val sessions = new AtomicReference(Map.empty[SessionKey, Future[CouchbaseSession]])

  /**
   * Scala API: Get an existing session or start a new one with the given settings and bucket name,
   * makes it possible to share one session across plugins.
   *
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CouchbaseSession manually instead.
   */
  def sessionFor(settings: CouchbaseSessionSettings, bucketName: String): Future[CouchbaseSession] =
    settings.enriched.flatMap { enrichedSettings =>
      val key = SessionKey(enrichedSettings, bucketName)
      sessions.get.get(key) match {
        case Some(futureSession) => futureSession
        case _ => startSession(key)
      }
    }(system.dispatcher)

  /**
   * Java API: Get an existing session or start a new one with the given settings and bucket name,
   * makes it possible to share one session across plugins.
   *
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CouchbaseSession manually instead.
   */
  def getSessionFor(settings: CouchbaseSessionSettings, bucketName: String): CompletionStage[JCouchbaseSession] =
    sessionFor(settings, bucketName)
      .map(_.asJava)(ExecutionContext.parasitic)
      .asJava

  @tailrec
  private def startSession(key: SessionKey): Future[CouchbaseSession] = {
    val promise = Promise[CouchbaseSession]()
    val oldSessions = sessions.get()
    val newSessions = oldSessions.updated(key, promise.future)
    if (sessions.compareAndSet(oldSessions, newSessions)) {
      // we won cas, initialize session
      val session = clusterRegistry
        .clusterFor(key.settings)
        .flatMap(cluster => CouchbaseSession(cluster, key.bucketName)(blockingDispatcher))(
          ExecutionContext.parasitic
        )
      promise.completeWith(session)
      promise.future
    } else {
      // we lost cas (could be concurrent call for some other key though), retry
      startSession(key)
    }
  }

}
