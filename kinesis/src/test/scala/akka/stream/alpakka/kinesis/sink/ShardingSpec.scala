package akka.stream.alpakka.kinesis.sink

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.services.kinesis.model.{HashKeyRange, Shard}

import java.math.BigInteger
import scala.util.Random

class ShardingSpec extends AnyWordSpec with Matchers {
  "Sharder" should {
    "sort shards by starting key ascendingly" in {
      val shards = (0 to 9).map { i =>
        val start = i * 10
        val end = start + 9
        val keyRange = HashKeyRange.builder().startingHashKey(start.toString).endingHashKey(end.toString).build()
        Shard.builder().shardId(i.toString).hashKeyRange(keyRange).build()
      }

      val sharder = Sharding(Random.shuffle(shards))

      for (i <- 0 to 99)
        sharder.getShard(BigInteger.valueOf(i)) shouldBe i / 10
    }
  }
}
