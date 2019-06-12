/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.scaladsl

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Measures the time to parse a 1 MB CSV file, consuming the first field of each row.
 *
 * ==Using Oracle Flight Recorder==
 * To record a Flight Recorder file from a JMH run, run it using the jmh.extras.JFR profiler:
 * > csv-bench/jmh:run -prof jmh.extras.JFR -t1 -f1 -wi 5 -i 10 .*CsvBench
 *
 * This will result in flight recording file which you can open and analyze offline using JMC.
 * Start with "jmc" from a terminal.
 *
 * ==Sample benchmark results==
 * Your results may differ.
 * Rerun these on YOUR OWN MACHINE before/after making changes.
 *
 * {{{
 * > csv-bench/jmh:run -t1 -f1 -wi 10 -i 10 .*CsvBench
 * [info] Benchmark        (bsSize)   Mode  Cnt    Score   Error  Units
 * [info] CsvBench.parse        32  thrpt   10   78.424 ± 1.351  ops/s
 * [info] CsvBench.parse      1024  thrpt   10  158.948 ± 3.187  ops/s
 * [info] CsvBench.parse      8192  thrpt   10  167.617 ± 2.759  ops/s
 * [info] CsvBench.parse     65536  thrpt   10  170.670 ± 2.462  ops/s
 * }}}
 *
 * @see https://github.com/ktoso/sbt-jmh
 */
@Warmup(iterations = 8, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(jvmArgsAppend = Array("-Xmx350m", "-XX:+HeapDumpOnOutOfMemoryError"), value = 1)
@State(Scope.Benchmark)
class CsvBench {

  implicit val system = ActorSystem()
  implicit val executionContext = system.dispatcher
  implicit val mat = ActorMaterializer()

  /**
   * Size of [[ByteString]] chunks in bytes.
   *
   * Total message size remains the same.
   * This just determines how big the chunks are.
   *
   * WSClient returns a Source[ByteString, _] in 8k chunks.
   */
  @Param(
    Array(
      "32", //   smaller than a field
      "1024", // ~8x smaller than row
      "8192", // ~same size as row
      "65536" // ~8k larger than row
    )
  )
  var bsSize: Int = _
  var source: Source[ByteString, NotUsed] = _

  @Benchmark
  def parse(bh: Blackhole): Unit = {
    val futureDone = {
      source
        .via(CsvParsing.lineScanner())
        .runForeach { fields =>
          bh.consume(fields.head.utf8String)
        }
    }
    Await.result(futureDone, Duration.Inf)
  }

  @TearDown
  def tearDown(): Unit = {
    mat.shutdown()
    system.terminate()
  }

  @Setup
  def setup(): Unit = {

    /**
     * 8 fields in a row, each of size 100, with commas and an '\n' is 8008 bytes.
     */
    val row = ByteString(('a' to 'h').map(_.toString * 100).mkString("", ",", "\n"))

    val allChunks = Iterator
      .continually(row)
      .take(1024 * 1024 / row.length) // approx 1MiB for easy conversion from ops/s
      .reduce(_ ++ _)
      /**
       * Reframe into 8KiB chunks to mimic WSClient, and
       * so csv boundaries misalign with ByteString chunks as they would in reality.
       */
      .grouped(bsSize)
      /**
       * Compact is important here.
       *
       * alpakka-csv perf suffers a bit over non-compact [[akka.util.ByteString.ByteStrings]].
       * Its design relies on getting bytes by index from the [[ByteString]].
       * The indirections add up.
       *
       * WSClient will return [[akka.util.ByteString.ByteString1C]] chunks,
       * so we should do the same.
       */
      .map(_.compact)
      .toIndexedSeq

    source = Source.fromIterator(() => allChunks.iterator)
  }
}

/**
 * For debugging.
 */
object CsvBench {

  def main(args: Array[String]): Unit = {
    val bench = new CsvBench
    bench.parse(
      new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.")
    )
    bench.tearDown()
  }
}
