package akka.stream.alpakka.kinesis.sink.aggregation

import akka.stream.alpakka.kinesis.KinesisLimits._
import akka.stream.alpakka.kinesis.sink.aggregation.AggRecord._
import com.google.protobuf.{ByteString, CodedInputStream}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.kinesis.retrieval.kpl.Messages.AggregatedRecord

import java.math.BigInteger
import scala.jdk.CollectionConverters._

class AggRecordSpec extends AnyWordSpec with Matchers {
  "AggRecord" must {
    "throw at invalid partition key" in {
      val aggRecord = new AggRecord
      val validKeys = (1 to 256).map("a" * _)
      val invalidKeys = Seq("", "a" * 257)
      val value = ByteString.copyFromUtf8("value")
      validKeys.foreach(aggRecord.addUserRecord(_, None, value))
      for (key <- invalidKeys) {
        the[IllegalArgumentException].thrownBy(aggRecord.addUserRecord(key, None, value))
      }
    }

    "throw at invalid explicit hash code" in {
      val aggRecord = new AggRecord
      val validHashCodes = Seq(MinExplicitHashCode, MaxExplicitHashCode)
      val invalidHashCodes = Seq(BigInteger.valueOf(-1), MaxExplicitHashCode.add(BigInteger.ONE))
      val key = "key"
      val value = ByteString.copyFromUtf8("value")
      validHashCodes.foreach(hashCode => aggRecord.addUserRecord(key, Some(hashCode), value))
      for (hashCode <- invalidHashCodes) {
        the[IllegalArgumentException] thrownBy aggRecord.addUserRecord(key, Some(hashCode), value)
      }
    }

    "aggregate user records" in {
      val aggRecord = new AggRecord
      val records = Seq(
        ("key1", None, "value1"),
        ("key2", Some(BigInteger.ZERO), "value2"),
        ("key1", None, "value3"),
        ("key3", Some(BigInteger.ONE), "value4"),
        ("key4", Some(BigInteger.ZERO), "value5")
      )

      records.foreach {
        case (key, hash, value) =>
          aggRecord.addUserRecord(key, hash, ByteString.copyFromUtf8(value)) mustBe empty
      }

      val request = aggRecord.buildRequest().get
      request.partitionKey() mustBe "key1"
      request.explicitHashKey() mustBe "0"

      val data = request.data().asByteArray()
      val input = CodedInputStream.newInstance(data, MagicBytes.length, data.length - MagicBytes.length - DigestLength)
      val aggregatedRecord = AggregatedRecord.parseFrom(input)
      aggregatedRecord.getPartitionKeyTableCount mustBe 4
      aggregatedRecord.getExplicitHashKeyTableCount mustBe 2
      aggregatedRecord.getRecordsCount mustBe 5

      aggregatedRecord.getRecordsList.asScala.map { record =>
        val partitionKey = aggregatedRecord.getPartitionKeyTable(record.getPartitionKeyIndex.toInt)
        val explicitHashKey =
          if (record.hasExplicitHashKeyIndex)
            Some(aggregatedRecord.getExplicitHashKeyTable(record.getExplicitHashKeyIndex.toInt))
          else None
        val value = record.getData.toStringUtf8
        (partitionKey, explicitHashKey.map(new BigInteger(_)), value)
      } mustBe records
    }

    "throw if user record is larger than max size of PutRecordRequest" in {
      val aggRecord = new AggRecord
      val key = "key"
      val value = "0" * (MaxBytesPerRecord - key.length + 1)
      the[IllegalArgumentException] thrownBy aggRecord.addUserRecord(key, None, ByteString.copyFromUtf8(value))
    }

    "build a request for the user record if it is larger than what can be aggregated" in {
      val aggRecord = new AggRecord
      val key = "key"
      val value = "0" * (MaxBytesPerRecord - key.length)
      aggRecord.addUserRecord(key, None, ByteString.copyFromUtf8(value)) mustBe Seq(
        PutRecordsRequestEntry.builder().partitionKey(key).data(SdkBytes.fromUtf8String(value)).build()
      )
    }

    "build a request for existing data and add the user record" in {
      val preferredRecordSize = 2048
      val aggRecord = new AggRecord(preferredRecordSize)
      val key = "key"
      val value = ByteString.copyFromUtf8("0" * (preferredRecordSize / 2))
      aggRecord.addUserRecord(key, None, value) mustBe empty
      val request = aggRecord.buildRequest().get
      aggRecord.addUserRecord(key, None, value) mustBe empty
      aggRecord.addUserRecord(key, None, value) mustBe Seq(request)
      aggRecord.buildRequest() mustBe Some(request)
    }

    "build one request for existing data and one for the user record" in {
      val preferredRecordSize = 2048
      val aggRecord = new AggRecord(preferredRecordSize)
      val key = "key"
      val value = ByteString.copyFromUtf8("0" * (preferredRecordSize / 2))
      val largeValue = "0" * (MaxBytesPerRecord - key.length)
      aggRecord.addUserRecord(key, None, value) mustBe empty
      val request = aggRecord.buildRequest().get
      aggRecord.addUserRecord(key, None, value) mustBe empty
      aggRecord.addUserRecord(key, None, ByteString.copyFromUtf8(largeValue)) mustBe Seq(
        request,
        PutRecordsRequestEntry.builder().partitionKey(key).data(SdkBytes.fromUtf8String(largeValue)).build()
      )
    }
  }
}
