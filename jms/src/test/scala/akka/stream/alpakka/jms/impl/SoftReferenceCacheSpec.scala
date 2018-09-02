/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class SoftReferenceCacheSpec extends WordSpec with Matchers {

  "soft reference cache lookup" should {
    "return default value on miss" in {
      val cache = new SoftReferenceCache[Int, String]
      cache.lookup(1, "one") shouldBe "one"
    }

    "return previous value on hit" in {
      val cache = new SoftReferenceCache[Int, String]
      cache.lookup(1, "one")
      cache.lookup(1, "two") shouldBe "one"
    }

    "remove entries on garbage collection" in {
      val cache = new SoftReferenceCache[Int, Array[Byte]]

      val deadline = System.currentTimeMillis() + 1.minute.toMillis // try for 1 minute.
      var i = 2

      def fill(key: Int): Unit = cache.lookup(key, Array.fill(40 * 1024 * 1024)(1.toByte))

      def fill100(): Unit = for (_ <- 1 to 100) {
        fill(i)
        i += 1
      }

      fill(1) // fill with old value, whose length is not 1024.
      val newValue = Array.fill(1024)(1.toByte)
      // we try to get value for key 1 collected by gc.
      // while the entry for key 1 is the old one, add entries to the cache,
      // and ask the JVM for a gc (pretty please).
      while (cache.lookup(1, newValue).length != 1024 && System.currentTimeMillis() < deadline) {
        fill100()
        System.gc()
      }

      cache.lookup(1, newValue).length shouldBe 1024
    }

    "makes entries visible across threads" in {
      val cache = new SoftReferenceCache[Int, String]
      val ex = Executors.newFixedThreadPool(32)
      implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

      val failure = new AtomicBoolean()

      cache.lookup(1, "1")

      def chain(predecessor: Future[Any], item: Int): Future[Any] =
        if (item == 100000)
          predecessor
        else
          chain(
            predecessor.map { _ =>
              // set own value
              cache.lookup(item, item.toString)
              // lookup value to be set by a predecessor thread and try to inject a wrong value
              val value = cache.lookup(item - 1, "wrong")
              if (value != (item - 1).toString) {
                // successfully injected wron value, that is a test failure.
                info(s"cache has value '$value' for entry ${item - 1}, expected '${item - 1}'")
                failure.set(true)
              }
            },
            item + 1
          )
      try {
        Await.ready(chain(Future.successful(""), 2), 10.seconds)
      } finally {
        ex.shutdownNow()
      }

      failure.get() shouldBe false
    }
  }
}
