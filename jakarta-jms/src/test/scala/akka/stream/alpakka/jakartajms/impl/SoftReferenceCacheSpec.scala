/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms.impl
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class SoftReferenceCacheSpec extends AnyWordSpec with Matchers {

  "soft reference cache lookup" should {
    "return default value on miss" in {
      val cache = new SoftReferenceCache[Int, String]
      cache.lookup(1, "one") shouldBe "one"
    }

    "return previous value on hit" in {
      System.gc() // if memory pressure exists, reduce it now.
      val cache = new SoftReferenceCache[Int, String]
      cache.lookup(1, "one")
      cache.lookup(1, "two") shouldBe "one"
    }

    "not evaluate default value on hit" in {
      System.gc() // if memory pressure exists, reduce it now.
      val cache = new SoftReferenceCache[Int, String]
      cache.lookup(1, "one")
      cache.lookup(1, throw new RuntimeException("Should not be evaluated")) shouldBe "one"
    }

    "remove entries on garbage collection" in {
      val cache = new SoftReferenceCache[Int, Array[Byte]]

      val deadline = System.currentTimeMillis() + 1.minute.toMillis // try for 1 minute.
      var i = 1

      def addCacheEntries(): Unit = for (_ <- 1 to 40) {
        cache.lookup(i, new Array[Byte](1024 * 1024))
        i += 1
      }

      val newValue = Array.fill(1024)(1.toByte)

      // detect eviction by inserting a different value into the cache for a previously set key.
      def entryEvicted(index: Int): Boolean = cache.lookup(index, newValue).length == 1024

      def noEntryEvicted: Boolean = !(1 until i).exists(entryEvicted)

      while (noEntryEvicted && System.currentTimeMillis() < deadline) {
        addCacheEntries()
        System.gc()
      }

      noEntryEvicted shouldBe false
    }

    "not need synchronization in intended usage scenario" in {
      // simulates the JmsProducerStage / JmsMessageProducer behavior to give some
      // evidence that the cache synchronization isn't needed when used in JmsMessageProducer

      // setup utilities
      implicit val ec: ExecutionContextExecutorService =
        ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(16))

      val stop = new AtomicBoolean(false)
      val failed = new AtomicBoolean(false)

      // setup cache under test
      type Cache = SoftReferenceCache[Long, String]
      class State(val cache: Cache = new Cache, var counter: Long = 0L)
      val ref = new AtomicReference(Option(new State()))
      ref.get.get.cache.lookup(0L, "0")

      // dequeue/enqueue simulates memory visibility guarantees of Akka's async callbacks
      def dequeue(): Option[State] = {
        val seen = ref.get
        seen.filter(_ => ref.compareAndSet(seen, None))
      }

      def enqueue(state: State): Unit = ref.set(Some(state))

      // run test
      for (_ <- 1 to 4)
        Future {
          while (!stop.get()) {
            dequeue().foreach { state =>
              val count = state.counter + 1
              val cache = state.cache
              Future {
                // no atomic reference operations on the happy path of the future itself
                val past = cache.lookup(count - 1, "wrong")
                cache.lookup(count, count.toString)
                if (past == "wrong") {
                  info(s"Worker did not see past update on key '${count - 1}' and was able to set wrong cache entry")
                  // note that these atomic reference operations are only executed when something went wrong already
                  failed.set(true)
                  stop.set(true)
                }
                state.counter = count
                state
              }.foreach(enqueue)(akka.dispatch.ExecutionContexts.parasitic)
            }
          }
        }

      // stop test
      Future {
        Thread.sleep(10.seconds.toMillis)
        stop.set(true)
      }

      Thread.sleep(9.seconds.toMillis)
      while (!stop.get()) {
        Thread.sleep(100)
      }
      ec.shutdown()

      while (ref.get.isEmpty) {
        Thread.sleep(10)
      }

      info(s"Executed ${ref.get.get.counter} cache lookups")

      // verify
      if (failed.get()) {
        fail("Synchronization was broken")
      }
    }
  }
}
