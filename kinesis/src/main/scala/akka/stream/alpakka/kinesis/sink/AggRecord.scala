package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisLimits._
import com.google.protobuf.{ByteString, CodedOutputStream}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.kinesis.retrieval.kpl.Messages.{AggregatedRecord, Record}

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import scala.collection.mutable

case class Aggregated(
  partitionKey: String,
  explicitHashKey: Option[String],
  data: SdkBytes,
  aggregatedRecords: Int,
  payloadBytes: Int
)

object AggRecord {
  val MagicBytes: Array[Byte] = Array[Byte](0xf3.toByte, 0x89.toByte, 0x9a.toByte, 0xc2.toByte)
  val DigestLength: Int = MD5.threadLocal(_.getDigestLength)
  private val ValidPartitionKeyLength = 1 to 256
  private val MaxAggregatedMessageSize = MaxBytesPerRecord - MagicBytes.length - DigestLength
}

class AggRecord(preferredRecordSize: Int = MaxBytesPerRecord) {
  import AggRecord._

  require(preferredRecordSize <= MaxBytesPerRecord)

  private val preferredAggregatedMessageSize = preferredRecordSize - MagicBytes.length - DigestLength
  private val aggregatedRecordBuilder = AggregatedRecord.newBuilder()
  private val partitionKeys = mutable.Map.empty[ByteString, Int]
  private val explicitHashKeys = mutable.Map.empty[String, Int]

  private var aggregatedMessageSize = 0
  private var aggRecordPartitionKey: Option[String] = None
  private var aggRecordPartitionKeyBytes: Int = 0
  private var aggRecordExplicitHashKey: Option[String] = None

  def addUserRecord(partitionKey: String,
                    explicitHashCode: Option[BigInteger],
                    data: ByteString): Seq[Aggregated] = {
    require(
      ValidPartitionKeyLength.contains(partitionKey.length),
      s"Expected partition key length [1, 256], but got ${partitionKey.length}"
    )

    for (hash <- explicitHashCode) {
      require(
        hash.compareTo(MinExplicitHashCode) >= 0 && hash.compareTo(MaxExplicitHashCode) <= 0,
        s"Invalid explicit hash key: $hash"
      )
    }

    val partitionKeyBytes = ByteString.copyFromUtf8(partitionKey)

    require(
      partitionKeyBytes.size() + data.size() <= MaxBytesPerRecord,
      s"Payload larger than 1 MB (Partition key length: ${partitionKeyBytes.size()}, data length: ${data.size()})"
    )

    val explicitHashKey = explicitHashCode.map(_.toString)

    def tryAddUserRecord(): Seq[Aggregated] = {
      val newRecordSize = calculateRecordSize(partitionKeyBytes, explicitHashKey, data)
      val aggregatedPayloadBytes = aggRecordPartitionKeyBytes + aggregatedMessageSize + newRecordSize
      if (aggregatedPayloadBytes > MaxAggregatedMessageSize && aggregatedRecordBuilder.getRecordsCount == 0) {
        val sdkBytes = SdkBytes.fromByteBuffer(data.asReadOnlyByteBuffer())
        val payloadBytes = partitionKeyBytes.size() + data.size()
        Seq(buildAggregated(partitionKey, explicitHashKey, sdkBytes, 1, payloadBytes))
      } else if (aggregatedPayloadBytes > preferredAggregatedMessageSize) {
        aggregate() +: tryAddUserRecord()
      } else {
        val newRecord = Record
          .newBuilder()
          .setData(data)
          .setPartitionKeyIndex(
            partitionKeys.getOrElseUpdate(partitionKeyBytes, {
              aggregatedRecordBuilder.addPartitionKeyTableBytes(partitionKeyBytes)
              aggregatedRecordBuilder.getPartitionKeyTableCount - 1
            })
          )

        for (ehk <- explicitHashKey) {
          newRecord.setExplicitHashKeyIndex(
            explicitHashKeys.getOrElseUpdate(ehk, {
              aggregatedRecordBuilder.addExplicitHashKeyTable(ehk)
              aggregatedRecordBuilder.getExplicitHashKeyTableCount - 1
            })
          )
        }

        aggregatedRecordBuilder.addRecords(newRecord)
        aggregatedMessageSize += newRecordSize
        if (aggRecordPartitionKey.isEmpty) {
          aggRecordPartitionKey = Some(partitionKey)
          aggRecordPartitionKeyBytes = partitionKeyBytes.size()
        }
        if (aggRecordExplicitHashKey.isEmpty) aggRecordExplicitHashKey = explicitHashKey
        Nil
      }
    }

    tryAddUserRecord()
  }

  private def calculateRecordSize(partitionKey: ByteString, explicitHashKey: Option[String], data: ByteString): Int = {
    val SizeOfMessageIndexAndWireType = 1
    var messageSize = 0
    var innerRecordSize = 0

    def sizeOfEncodedData(dataLength: Int): Int =
      SizeOfMessageIndexAndWireType + calculateVarintSize(dataLength) + dataLength
    def sizeOfEncodedInt(value: Int): Int = SizeOfMessageIndexAndWireType + calculateVarintSize(value)

    val partitionKeyIndex = partitionKeys.getOrElse(partitionKey, {
      messageSize += sizeOfEncodedData(partitionKey.size())
      aggregatedRecordBuilder.getPartitionKeyTableCount
    })

    val explicitHashKeyIndex = explicitHashKey.map { ehk =>
      explicitHashKeys.getOrElse(ehk, {
        messageSize += sizeOfEncodedData(ehk.length)
        aggregatedRecordBuilder.getExplicitHashKeyTableCount
      })
    }

    innerRecordSize += sizeOfEncodedInt(partitionKeyIndex)
    innerRecordSize += explicitHashKeyIndex.map(sizeOfEncodedInt).getOrElse(0)
    innerRecordSize += sizeOfEncodedData(data.size())

    messageSize + sizeOfEncodedData(innerRecordSize)
  }

  private def calculateVarintSize(value: Long): Int = {
    import java.lang.{Long => JLong}
    val bitsNeeded = (JLong.SIZE - JLong.numberOfLeadingZeros(value)).max(1)
    bitsNeeded / 7 + (if (bitsNeeded % 7 > 0) 1 else 0)
  }

  def aggregate(): Aggregated = {
    assert(aggregatedRecords > 0)
    val partitionKey = aggRecordPartitionKey.get
    val explicitHashKey = aggRecordExplicitHashKey
    val data = SdkBytes.fromByteArrayUnsafe(recordBytes())
    val payloadBytes = aggregatedBytes + aggRecordPartitionKeyBytes
    val aggregated = buildAggregated(partitionKey, explicitHashKey, data, aggregatedRecords, payloadBytes)
    clear()
    aggregated
  }

  private def buildAggregated(partitionKey: String,
                              explicitHashKey: Option[String],
                              data: SdkBytes,
                              aggregatedRecords: Int,
                              payloadBytes: Int): Aggregated = {
    assert(payloadBytes <= MaxBytesPerRecord)
    Aggregated(partitionKey, explicitHashKey, data, aggregatedRecords, payloadBytes)
  }

  private def recordBytes(): Array[Byte] = {
    val messageBytes = new Array[Byte](MagicBytes.length + aggregatedMessageSize + DigestLength)
    System.arraycopy(MagicBytes, 0, messageBytes, 0, MagicBytes.length)
    aggregatedRecordBuilder
      .build()
      .writeTo(CodedOutputStream.newInstance(messageBytes, MagicBytes.length, aggregatedMessageSize))
    MD5.threadLocal { md5 =>
      md5.update(messageBytes, MagicBytes.length, aggregatedMessageSize)
      md5.digest(messageBytes, MagicBytes.length + aggregatedMessageSize, DigestLength)
      messageBytes
    }
  }

  private def clear(): Unit = {
    aggRecordPartitionKey = None
    aggRecordPartitionKeyBytes = 0
    aggRecordExplicitHashKey = None
    aggregatedMessageSize = 0
    partitionKeys.clear()
    explicitHashKeys.clear()
    aggregatedRecordBuilder.clear()
  }

  def aggregatedBytes: Int = {
    if (aggregatedMessageSize == 0) 0
    else MagicBytes.length + aggregatedMessageSize + DigestLength
  }

  def aggregatedRecords: Int = {
    aggregatedRecordBuilder.getRecordsCount
  }
}
