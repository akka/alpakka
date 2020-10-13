/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.geode

import org.apache.geode.cache.client.ClientCacheFactory

/**
 * General settings to connect to Apache Geode.
 */
final class GeodeSettings private (val hostname: String,
                                   val port: Int = 10334,
                                   val configure: Option[ClientCacheFactory => ClientCacheFactory] = None,
                                   val pdxCompat: (Class[_], Class[_]) => Boolean = (c1, c2) =>
                                     c1.getSimpleName equals c2.getSimpleName
) {

  private def copy(hostname: String = hostname,
                   port: Int = port,
                   configure: Option[ClientCacheFactory => ClientCacheFactory] = configure,
                   pdxCompat: (Class[_], Class[_]) => Boolean = pdxCompat
  ) =
    new GeodeSettings(hostname, port, configure, pdxCompat)

  /**
   * @param configure function to configure geode client factory
   */
  def withConfiguration(configure: ClientCacheFactory => ClientCacheFactory): GeodeSettings =
    copy(configure = Some(configure))

  /**
   * @param pdxCompat a function that determines if two class are equivalent (java class / scala case class)
   */
  def withPdxCompat(pdxCompat: (Class[_], Class[_]) => Boolean): GeodeSettings = copy(pdxCompat = pdxCompat)

  override def toString: String =
    "GeodeSettings(" +
    s"hostname=$hostname," +
    s"port=$port," +
    s"configuration=${configure.isDefined}" +
    ")"

}

object GeodeSettings {

  def apply(hostname: String, port: Int = 10334): GeodeSettings = new GeodeSettings(hostname, port)

  def create(hostname: String): GeodeSettings = new GeodeSettings(hostname)

  def create(hostname: String, port: Int): GeodeSettings = new GeodeSettings(hostname, port)

}

final class RegionSettings[K, V] private (val name: String, val keyExtractor: V => K) {
  override def toString: String =
    "RegionSettings(" +
    s"name=$name" +
    ")"
}

object RegionSettings {

  def apply[K, V](name: String, keyExtractor: V => K): RegionSettings[K, V] = new RegionSettings(name, keyExtractor)

  def create[K, V](name: String, keyExtractor: V => K): RegionSettings[K, V] =
    new RegionSettings(name, keyExtractor)

}
