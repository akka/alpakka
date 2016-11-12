/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.caffeine

import scala.concurrent.duration.FiniteDuration

/**
 * Aggregation contract to:
 * <ul>
 * <li>Initializes a new aggregate: when a element with a new correlation id (cache key) hits the flow</li>
 * <li>Extracts the correlation id from incoming/outgoing element</li>
 * <li>Combines an incoming element with current aggregate</li>
 * <li>Decides if aggregate is complete</li>
 * <li>Defines the retention time (TTL) of element in the cache</li>
 * </ul>
 *
 * @tparam K key in the cache
 * @tparam V value in the cache
 * @tparam R aggregation type
 */
trait Aggregator[K, V, R] {

  /**
   * Initiate a fresh aggregate.
   *
   * @param key a new correlation id
   * @return a fresh aggregate with the key, containing not event.
   */
  def newAggregate(key: K): R

  /**
   * Extract the key from Event (in).
   *
   * @param v event to extract the key from.
   * @return the key from an event.
   */
  def inKey(v: V): K

  /**
   * Extract the key from Aggregate (out) event.
   *
   * @param r aggregate to extract the key from.
   * @return the key from aggregate
   */
  def outKey(r: R): K

  /**
   * Combine event and aggregate.
   *
   * @param r previous aggregate
   * @param v new event
   * @return aggregate r + v
   */
  def combine(r: R, v: V): R

  /**
   * Tells when aggregate in complete, thus can be emitted.
   *
   * @param r an aggregate
   * @return true when aggregation is complete
   */
  def isComplete(r: R): Boolean

  /**
   * TTL (after write)
   *
   * @return the TTL for event in cache.
   */
  def expiration: FiniteDuration
}
