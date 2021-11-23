/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.geode.RegionSettings
import akka.stream.stage.StageLogging
import org.apache.geode.cache.client.{ClientCache, ClientRegionShortcut}

import scala.util.control.NonFatal
@InternalApi
private[geode] trait GeodeCapabilities[K, V] { this: StageLogging =>

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
