/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode

import org.apache.geode.cache.client.ClientCacheFactory

/**
 * General settings to connect Apache Geode.
 *
 * @param hostname
 * @param port      default to 10334
 * @param pdxCompat a function that determines if two class are equivalent (java class / scala case class)
 */
case class GeodeSettings(hostname: String,
                         port: Int = 10334,
                         configure: Option[ClientCacheFactory => ClientCacheFactory] = None,
                         pdxCompat: (Class[_], Class[_]) => Boolean = (c1, c2) =>
                           c1.getSimpleName equals c2.getSimpleName) {

  /**
   * @param configure function to configure geode client factory
   */
  def withConfiguration(configure: ClientCacheFactory => ClientCacheFactory) =
    copy(configure = Some(configure))

  /**
   * @param pdxCompat a function that determines if two class are equivalent (java class / scala case class)
   */
  def withPdxCompat(pdxCompat: (Class[_], Class[_]) => Boolean) =
    copy(pdxCompat = pdxCompat)
}

object GeodeSettings {
  def create(hostname: String) = new GeodeSettings(hostname)

  def create(hostname: String, port: Int) = new GeodeSettings(hostname, port)

}

case class RegionSettings[K, V](name: String, keyExtractor: V => K)
