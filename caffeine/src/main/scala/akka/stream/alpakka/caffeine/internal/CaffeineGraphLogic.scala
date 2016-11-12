/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.caffeine.internal

import akka.stream.alpakka.caffeine.Aggregator
import akka.stream.{Inlet, Outlet, Shape}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.annotation.tailrec
import scala.collection.mutable

abstract class CaffeineGraphLogic[K, V, R](shape: Shape, aggregator: Aggregator[K, V, R], in: Inlet[V])
    extends GraphStageLogic(shape)
    with InHandler {

  protected val completeQueue = mutable.Queue.empty[R]

  protected val expiredQueue = mutable.Queue.empty[R]

  protected var cache: Cache[K, R] = _

  var closing = false

  /**
   * Hold the keys currently in cache.
   * Needed to prune reconciled and expired aggregate.
   */
  private val onAir = mutable.Set[K]()

  protected def pumpExp(): Boolean

  protected def pumpComplete(): Boolean

  @tailrec final def pump(i: Boolean = false): Unit = {
    val a = if (i) pumpExp() else pumpComplete()
    val b = if (i) pumpComplete() else pumpExp()
    if (a || b) pump(!i)
  }

  override def preStart(): Unit = {

    val expired = getAsyncCallback[R]({ r =>
      if (onAir.remove(aggregator.outKey(r)))
        expiredQueue.enqueue(r)
      pump()
    })

    cache = Scaffeine()
      .expireAfterWrite(aggregator.expiration)
      .removalListener[K, R] {
        case (key, element, cause) =>
          cause match {
            case RemovalCause.EXPIRED =>
              expired.invoke(element)
            case _ =>
          }
      }
      .build[K, R]()

  }

  protected def checkComplete(outlet: Outlet[_]): Unit =
    if (closing && onAir.isEmpty)
      complete(outlet)

  setHandler(in, this)

  override def onPush(): Unit = {
    val a = grab(in)
    val key = aggregator.inKey(a)

    val old = cache.getIfPresent(key)

    val r = aggregator.combine(old.getOrElse(aggregator.newAggregate(key)), a)

    if (aggregator.isComplete(r)) {
      if (old.isDefined)
        cache.invalidate(key)
      onAir.remove(key)
      completeQueue.enqueue(r)
      cache.cleanUp()

    } else {
      if (old.isEmpty)
        onAir.add(key)
      cache.put(key, r)
    }
    pump()
  }

  override def onUpstreamFinish(): Unit = {

    while (cache.estimatedSize() > 0) {
      cache.cleanUp()
    }
    closing = true
    pump()
  }
}
