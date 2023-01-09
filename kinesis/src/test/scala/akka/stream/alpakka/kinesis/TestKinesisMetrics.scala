package akka.stream.alpakka.kinesis

import akka.stream.alpakka.kinesis.KinesisLimits._
import akka.stream.alpakka.kinesis.KinesisMetrics._
import com.codahale.metrics.{MetricRegistry, Snapshot, Gauge => CodahaleGauge}

import java.util.concurrent.atomic.AtomicLong

object TestKinesisMetrics {
  private val Gauges = Seq(AggregationGroups, BufferedPayloadBytes).sortBy(_.name)

  private val Records = Seq(AggRecordSize, UserRecordSize, BatchRequestBytes).sortBy(_.name)

  private val Counters = Seq(
    UserRecordSuccess,
    UserRecordFailure,
    UserRecordDropped,
    UserRecordTooLarge,
    AggRecordSuccess,
    AggRecordFailure,
    AggRecordDropped,
    AggRecordRetryable,
    BytesSuccess,
    BytesFailure,
    BytesDropped,
    BytesRetryable,
    BytesSent
  ).sortBy(_.name)
}

class TestKinesisMetrics(nrOfShards: Int) extends KinesisMetrics {
  import TestKinesisMetrics._

  private val registry = new MetricRegistry()

  private val gauges = Gauges.map { metric =>
    val value = new AtomicLong(0)
    registry.gauge(metric.name, () => () => value.get())
    (metric, value)
  }.toMap

  override def count(metric: Counter, v: Long, tags: Map[String, String]): Unit =
    registry.counter(metric.name).inc(v)

  override def gauge(metric: Gauge, v: Long, tags: Map[String, String]): Unit = {
    registry.gauge(metric.name, () => () => gauges.get(metric).map(_.get()).getOrElse(0L))
    gauges(metric).set(v)
  }

  override def record(metric: Record, v: Long, tags: Map[String, String]): Unit =
    registry.histogram(metric.name).update(v)

  def counterValue(metric: Counter): Long = registry.counter(metric.name).getCount

  def gaugeValue(metric: Gauge): Long = registry.getMetrics.get(metric.name).asInstanceOf[CodahaleGauge[Long]].getValue

  def snapshot(metric: Record): Snapshot = registry.histogram(metric.name).getSnapshot

  object Sample {
    def empty: Sample = Sample(System.nanoTime(), Seq.fill(Counters.size)(0L), Seq.fill(Gauges.size)(0L), Nil)
  }

  case class Sample(timestamp: Long, counters: Seq[Long], gauges: Seq[Long], records: Seq[Snapshot]) {
    def diff(old: TestKinesisMetrics#Sample): Progress = Progress(
      elapsed = (timestamp - old.timestamp) / 1e9f,
      counters = (0 until Counters.size)
        .map(i => (Counters(i), (counters(i), old.counters(i))))
        .toMap,
      gauges = (0 until Gauges.size)
        .map(i => (Gauges(i), (gauges(i), old.gauges(i))))
        .toMap,
      records = (0 until Records.size)
        .map(i => (Records(i), records(i)))
        .toMap
    )
  }

  case class Progress(
      elapsed: Float,
      counters: Map[Counter, (Long, Long)],
      gauges: Map[Gauge, (Long, Long)],
      records: Map[Record, Snapshot]
  ) {
    def countOf(metric: Counter): Long = counters.get(metric).map(_._1).getOrElse(0L)
    def valueOf(metric: Gauge): Long = gauges.get(metric).map(_._1).getOrElse(0L)
    def countDiff(metric: Counter): Long = counters.get(metric).map(e => e._1 - e._2).getOrElse(0L)

    def meanRate(metric: Counter): Float = {
      val (curr, old) = counters.getOrElse(metric, (0L, 0L))
      (curr - old) / elapsed
    }

    def print(): Unit = {
      printf("%-36s: %12.6f s\n", "time elapsed", elapsed)

      def printDelta(metric: String, curr: Long, last: Long): Option[Float] = {
        if (curr == 0) None
        else {
          val delta = (curr - last).toFloat
          val (value, acc, unit) =
            if (!metric.contains("bytes")) (delta, curr.toFloat, "")
            else (delta / (1 << 20), curr.toFloat / (1 << 20), "MiB")
          printf("%-36s: %9.3f %3s/s, acc: %10.3f %s\n", metric, value / elapsed, unit, acc, unit)
          Some(delta)
        }
      }

      counters.foreach { case (m, (curr, old)) => printDelta(m.name, curr, old) }
      val success = countDiff(BytesSuccess)
      val retryable = countDiff(BytesRetryable)
      printf("%-36s: %9.3f %%\n", "retryable / total", retryable.toFloat / (retryable + success) * 100)
      printf(
        "%-36s: %9.3f %%\n",
        "success / bandwidth",
        success / elapsed / (MaxBytesPerSecondPerShard * nrOfShards) * 100
      )

      gauges.foreach { case (m, (cur, _)) => printf("%-36s: %9d\n", m, cur) }

      records.foreach {
        case (m, s) =>
          printf(s"%-36s: %4d records, min/mean/max: %7d / %10.2f / %7d\n", m, s.size(), s.getMin, s.getMean, s.getMax)
      }
      println("-" * 96)
    }
  }

  def sample(): Sample = Sample(
    System.nanoTime(),
    Counters.map(m => registry.counter(m.name).getCount),
    Gauges.map(m => registry.getMetrics.get(m.name).asInstanceOf[CodahaleGauge[Long]].getValue),
    Records.map { m =>
      val snapshot = registry.histogram(m.name).getSnapshot
      registry.remove(m.name)
      snapshot
    }
  )
}
