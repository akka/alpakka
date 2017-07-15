/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.internal

import akka.stream.alpakka.geode.RegionSettings
import akka.stream.stage.StageLogging
import org.apache.geode.cache.client.{ClientCache, ClientRegionShortcut}

import scala.util.control.NonFatal

trait GeodeCapabilities[K, V] { this: StageLogging =>

  def regionSettings: RegionSettings[K, V]

  def clientCache: ClientCache

  private lazy val region =
    clientCache.createClientRegionFactory[K, V](ClientRegionShortcut.CACHING_PROXY).create(regionSettings.name)

  def put(v: V): Unit = region.put(regionSettings.keyExtractor(v), v)

  def close(): Unit =
    try {
      if (clientCache.isClosed)
        return
      region.close()
      log.debug("region closed")
    } catch {
      case NonFatal(ex) => log.error(ex, "Problem occurred during producer region closing")
    }
}
