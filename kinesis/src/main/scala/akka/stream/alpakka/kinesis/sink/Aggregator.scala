/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisMetrics
import akka.stream.alpakka.kinesis.KinesisMetrics._
import com.google.protobuf.ByteString

import java.math.BigInteger
import scala.annotation.unused
import scala.collection.mutable
import scala.util.Try

object Aggregatable {
  implicit val keyValue: Aggregatable[(String, Array[Byte])] = new Aggregatable[(String, Array[Byte])] {
    override def getKey(record: (String, Array[Byte])): String = record._1
    override def getData(record: (String, Array[Byte])): ByteString = ByteString.copyFrom(record._2)
  }
  implicit val keyHashValue: Aggregatable[(String, BigInteger, Array[Byte])] =
    new Aggregatable[(String, BigInteger, Array[Byte])] {
      override def getKey(record: (String, BigInteger, Array[Byte])): String = record._1
      override def getExplicitHashKey(record: (String, BigInteger, Array[Byte])): Option[BigInteger] = Some(record._2)
      override def getData(record: (String, BigInteger, Array[Byte])): ByteString = ByteString.copyFrom(record._3)
    }
}
trait Aggregatable[T] {
  def getKey(record: T): String
  def getExplicitHashKey(@unused record: T): Option[BigInteger] = None
  def getData(record: T): ByteString
  def payloadSize(record: T): Int = getKey(record).length + getData(record).size()
}

object AggGroup {
  private case class Wrapped(key: Any) extends AggGroup
  val Empty: AggGroup = apply(None)
  def apply(key: Any): AggGroup = Wrapped(key)
}
trait AggGroup {
  def key: Any
  def tags: Map[String, String] = Map.empty

  final override def hashCode(): Int = key.hashCode()

  final override def equals(obj: Any): Boolean =
    obj match {
      case group: AggGroup => key == group.key
      case _ => false
    }
}

object Aggregator {
  type Factory[In] =
    (Int, Aggregatable[In], ShardMemoryLimit, KinesisSinkSettings, KinesisMetrics) => Aggregator[In]

  def default[T]: Factory[T] =
    by(_ => AggGroup.Empty)

  def by[T](groupKey: T => AggGroup): Factory[T] = {
    (
        shardIndex: Int,
        inType: Aggregatable[T],
        memoryLimit: ShardMemoryLimit,
        settings: KinesisSinkSettings,
        metrics: KinesisMetrics
    ) =>
      new GroupingAggregator(shardIndex, groupKey, memoryLimit, settings, metrics)(inType)
  }
}

trait Aggregator[In] {

  /** Add a user record that is to be aggregated. And return aggregated records
   * if either the size limit of a single aggregated record or the total buffer
   * size limit is hit.
   */
  def addUserRecord(record: In): Try[Iterable[Aggregated]]

  /** Get an aggregated record even if it does not hit the size limit. It is
   * called when downstream ask for data but the buffer is empty.
   */
  def getOne(): Aggregated

  /** if there's any data being aggregated
   */
  def isEmpty: Boolean
}

/** The default aggregator implementation that aggregates records by group keys.
 * It keeps a map of `AggRecord`s for every group key in aggregation. A user
 * record is add to the `AggRecord` whose map key matches its group key. The
 * `AggRecord` is removed from the map when it hits the size limit. If the
 * total size of record being aggregated/buffered exceeds the limit, all
 * `AggRecord`s will be removed from the map. Whenever and `Aggregated` record
 * is removed from the map, an `Aggregated` record will be built from it and
 * returned to the record processor.
 */
class GroupingAggregator[T: Aggregatable](
    shardId: Int,
    getGroupKey: T => AggGroup,
    memoryLimit: ShardMemoryLimit,
    settings: KinesisSinkSettings,
    metrics: KinesisMetrics
) extends Aggregator[T] {
  private val inType = implicitly[Aggregatable[T]]
  private val groups = mutable.LinkedHashMap.empty[AggGroup, AggRecord]

  override def addUserRecord(record: T): Try[Iterable[Aggregated]] = {
    val group = getGroupKey(record)
    val aggRecord = groups.getOrElseUpdate(group, new AggRecord(shardId, group, settings.maxAggRecordBytes))
    val result = Try(addRecord(aggRecord, record)).map { requests =>
      if (memoryLimit.shouldDropOrBackpressure) {
        val all = groups.map(_._2.aggregate())
        groups.clear()
        requests ++ all
      } else requests
    }
    if (aggRecord.aggregatedRecords == 0) groups.remove(group)
    metrics.gauge(AggregationGroups, groups.size)
    memoryLimit.sync(metrics)
    result
  }

  override def getOne(): Aggregated = {
    val group = groups.head._1
    val aggregated = groups.remove(group).get.aggregate()
    metrics.gauge(AggregationGroups, groups.size)
    aggregated
  }

  override def isEmpty: Boolean = groups.isEmpty

  /** Add a user record to an existing aggregation group if there is enough space.
   * Return whether the user record is added successfully.
   */
  private def addRecord(aggRecord: AggRecord, record: T): Seq[Aggregated] = {
    val payloadBytes = aggRecord.payloadBytes
    val partitionKey = inType.getKey(record)
    val explicitHashKey = inType.getExplicitHashKey(record)
    val data = inType.getData(record)
    val result = aggRecord.addUserRecord(partitionKey, explicitHashKey, data)
    memoryLimit.add(aggRecord.payloadBytes - payloadBytes + result.map(_.payloadBytes).sum)
    result
  }
}
