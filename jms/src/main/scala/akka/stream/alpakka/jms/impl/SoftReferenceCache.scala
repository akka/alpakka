/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl
import akka.annotation.InternalApi

import scala.collection.mutable
import scala.ref.SoftReference

/**
 * This cache is customized to the idiosyncrasies of Jms producer destination creation.
 * Especially, it relies on one-thread-at-a-time use. Here, writes needs to be made visible to the next
 * Thread. Other synchronization tasks are not necessary.
 *
 * Use in other places is not recommended.
 * see [[akka.stream.alpakka.jms.JmsMessageProducer]]
 *
 * @tparam K cache key type
 * @tparam V cache value type
 */
@InternalApi private[jms] class SoftReferenceCache[K, V <: AnyRef] {

  private val cache: mutable.HashMap[K, SoftReference[V]] = mutable.HashMap[K, SoftReference[V]]()

  // this counter serves only the purpose of generating memory fences.
  @volatile private var memoryFenceGenerator: Long = 0L

  def lookup(key: K, default: => V): V = {
    val lfence = memoryFenceGenerator // force a lfence (read memory barrier)
    cache.get(key) match {
      case Some(ref) =>
        ref.get match {
          case Some(value) => value
          case None =>
            purgeCache() // facing a garbage collected soft reference, purge other entries.
            update(key, default, lfence)
        }

      case miss => update(key, default, lfence)
    }
  }

  private def update(key: K, value: V, lfence: Long): V = {
    cache.put(key, new SoftReference(value))
    memoryFenceGenerator = lfence + 1 // force a sfence (write memory barrier) after update(s).
    value
  }

  private def purgeCache(): Unit =
    cache --= cache.collect { case (key, ref) if ref.get.isEmpty => key }.to[Vector]
}
