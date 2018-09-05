/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl
import akka.annotation.InternalApi

import scala.collection.mutable
import scala.ref.SoftReference

@InternalApi private[jms] class SoftReferenceCache[K, V <: AnyRef] {

  private val cache = mutable.HashMap[K, SoftReference[V]]()

  private val lock = new AnyRef

  def lookup(key: K, default: => V): V = lock.synchronized {
    cache.get(key) match {
      case Some(ref) =>
        ref.get match {
          case Some(value) => value
          case None =>
            purgeCache() // facing a garbage collected soft reference, purge other entries.
            update(key, default)
        }

      case None => update(key, default)
    }
  }

  private def update(key: K, value: V): V = {
    cache.put(key, new SoftReference(value))
    value
  }

  private def purgeCache(): Unit =
    cache --= cache.collect { case (key, ref) if ref.get.isEmpty => key }.to[Vector]
}
