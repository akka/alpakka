/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet

import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.FiniteDuration

object Parquet {

  case class ParquetSettings(baseUri: String,
                             writeMode: Mode = Mode.CREATE,
                             compressionCodeName: CompressionCodecName = CompressionCodecName.SNAPPY,
                             partitionSettings: Option[PartitionSettings] = None) {
    def withWriteMode(mode: Mode) = copy(writeMode = mode)

    def withCompressionCodecName(compressionCodec: CompressionCodecName) = copy(compressionCodeName = compressionCodec)

    def withPartitionSettings(partitionSettings: PartitionSettings) = copy(partitionSettings = Some(partitionSettings))
  }

  case class PartitionSettings(maxRecordsPerWindow: Int, windowSize: FiniteDuration, writeParallelism: Int)

}
