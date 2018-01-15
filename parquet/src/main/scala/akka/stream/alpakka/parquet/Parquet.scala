/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.parquet

import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.FiniteDuration

object Parquet {

  case class ParquetSettings(baseUri: String,
                             maxRecordsPerWindow: Int,
                             window: FiniteDuration,
                             writeMode: Mode = Mode.CREATE,
                             compressionCodeName: CompressionCodecName = CompressionCodecName.SNAPPY,
                             writeParallelism: Int = 1) {
    def withWriteMode(mode: Mode) = copy(writeMode = mode)

    def withCompressionCodecName(compressionCodec: CompressionCodecName) = copy(compressionCodeName = compressionCodec)

    def withWriteParallelism(parallelism: Int) = copy(writeParallelism = parallelism)
  }

}
