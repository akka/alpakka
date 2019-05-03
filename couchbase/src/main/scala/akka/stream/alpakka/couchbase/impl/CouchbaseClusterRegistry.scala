/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.couchbase.client.java.AsyncCluster

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}

/**
 * Internal API
 */
@InternalApi
final private[couchbase] class CouchbaseClusterRegistry(system: ActorSystem) {

  private val log = Logging(system, classOf[CouchbaseClusterRegistry])

  private val clusters = new AtomicReference(Map.empty[CouchbaseSessionSettings, Future[AsyncCluster]])

  def clusterFor(settings: CouchbaseSessionSettings): Future[AsyncCluster] =
    clusters.get.get(settings) match {
      case Some(futureSession) => futureSession
      case _ => startSession(settings)
    }

  @tailrec
  private def startSession(settings: CouchbaseSessionSettings): Future[AsyncCluster] = {
    val promise = Promise[AsyncCluster]()
    val oldClusters = clusters.get()
    val newClusters = oldClusters.updated(settings, promise.future)
    if (clusters.compareAndSet(oldClusters, newClusters)) {
      // we won cas, initialize session
      def nodesAsString = settings.nodes.mkString("\"", "\", \"", "\"")
      log.info("Starting Couchbase client for nodes [{}]", nodesAsString)
      promise.completeWith(CouchbaseSession.createClusterClient(settings)(system.dispatcher))
      val future = promise.future
      system.registerOnTermination {
        future.foreach { cluster =>
          val nodesAsString = settings.nodes.mkString("\"", "\", \"", "\"")
          log.info("Shutting down Couchbase client for nodes [{}]", nodesAsString)
          cluster.disconnect()
        }(system.dispatcher)
      }
      future
    } else {
      // we lost cas (could be concurrent call for some other settings though), retry
      startSession(settings)
    }
  }

}
