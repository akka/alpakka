/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.sink

import software.amazon.awssdk.services.kinesis.model.Shard

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.util

object Sharding {
  def apply(shards: Iterable[Shard]): Sharding = new Sharding {
    val nrOfShards: Int = shards.size

    private val sortedShards = shards
      .map(shard => new BigInteger(shard.hashKeyRange().startingHashKey()))
      .toArray[AnyRef]

    util.Arrays.sort(sortedShards)

    def getShard(hashKey: BigInteger): Int =
      util.Arrays.binarySearch(sortedShards, hashKey) match {
        case index if index >= 0 => index
        case insertAt => -insertAt - 2
      }
  }

  def createExplicitHashKey(partitionKey: String): BigInteger = {
    MD5.threadLocal { md5 =>
      val pkDigest = md5.digest(partitionKey.getBytes(StandardCharsets.UTF_8))
      (0 until pkDigest.length).foldLeft(BigInteger.ZERO) { (hashKey, i) =>
        val p = new BigInteger(String.valueOf(pkDigest(i).toInt & 0xFF))
        val shifted = p.shiftLeft((16 - i - 1) * 8)
        hashKey.add(shifted)
      }
    }
  }
}

trait Sharding {
  def nrOfShards: Int
  def getShard(hashKey: BigInteger): Int
}
