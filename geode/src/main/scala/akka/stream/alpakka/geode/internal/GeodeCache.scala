/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.internal

import akka.stream.alpakka.geode.GeodeSettings
import akka.stream.alpakka.geode.internal.pdx.DelegatingPdxSerializer
import org.apache.geode.cache.client.{ClientCache, ClientCacheFactory}
import org.apache.geode.pdx.PdxSerializer

/**
 * Base of all geode client.
 *
 */
abstract class GeodeCache(geodeSettings: GeodeSettings) {

  private lazy val serializer = new DelegatingPdxSerializer(geodeSettings.pdxCompat)

  protected def registerPDXSerializer[V](pdxSerializer: PdxSerializer, clazz: Class[V]): Unit =
    serializer.register(pdxSerializer, clazz)

  /**
   * This method will overloaded to provide server event subscription.
   *
   * @return
   */
  protected def configure(factory: ClientCacheFactory): ClientCacheFactory

  /**
   * Return ClientCacheFactory:
   * <ul>
   * <li>with PDX support</li>
   * <li>configured by sub classes</li>
   * <li>customized by client application</li>
   * </ul>
   *
   */
  final protected def newCacheFactory(): ClientCacheFactory = {
    val factory = configure(new ClientCacheFactory().setPdxSerializer(serializer))
    geodeSettings.configure.map(_(factory)).getOrElse(factory)
  }

  lazy val cache: ClientCache = newCacheFactory().create()

  def close(keepAlive: Boolean = false): Unit = cache.close(keepAlive)

}
