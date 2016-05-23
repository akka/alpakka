/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.util.zip.{ ZipEntry, ZipInputStream, ZipOutputStream }
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestDuration
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ZipInputStreamSourceSpecAutoFusingOn
  extends { val autoFusing = true } with ZipInputStreamSourceSpec
class ZipInputStreamSourceSpecAutoFusingOff
  extends { val autoFusing = false } with ZipInputStreamSourceSpec

trait ZipInputStreamSourceSpec extends BaseStreamSpec {

  "A ZipInputStreamSource" should {
    "emit as many chunks as files when files fit on the chunks" in {
      check(numFiles = 3)
    }

    "emit more chunks than files when files don't fit on the chunks" in {
      check(numFiles = 3, chunkSize = 256)
    }

    "emit an extra chunk per file when the distribution is not exact" in {
      check(numFiles = 3, chunkSize = 250)
    }

    "materialize to 0 bytes when the file is empty" in {
      check(numFiles = 0)
    }

    "fail when the zip cannot be read and materialize to an Exception" in {
      val (ex, probe) = ZipInputStreamSource(() => throw new Exception)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      probe
        .expectSubscriptionAndError()

      intercept[Exception] {
        Await.result(ex, 1.second.dilated)
      }
    }
  }

  private def check(numFiles: Int, fileSize: Int = 1024, chunkSize: Int = 1024) = {
    val expectedSeq =
      Vector.fill(numFiles)(loremIpsum.take(fileSize).toVector.grouped(chunkSize).toVector).flatten
    val nChunksPerFile = fileSize / chunkSize + (if (fileSize % chunkSize != 0) 1 else 0)
    val totalRequested = if (numFiles > 0) numFiles.toLong * nChunksPerFile else 1

    val (totalBytesRead, probe) =
      ZipInputStreamSource(() => new ZipInputStream(sampleZipFile(numFiles, fileSize)), chunkSize)
        .map { case (_, bs) => bs.toVector }
        .toMat(TestSink.probe)(Keep.both)
        .run()
    probe
      .request(totalRequested)
      .expectNextN(expectedSeq)
      .expectComplete()

    Await.result(totalBytesRead, 1.second.dilated) shouldBe (numFiles * fileSize)
  }

  private def sampleZipFile(
    numFiles:    Int,
    sizePerFile: Int = 1024,
    dirRatio:    Int = 4
  ): ByteArrayInputStream = {
    withZos { zos =>
      (1 to numFiles).foreach(i1 => {
        val dirName = if (i1 > dirRatio) s"directory_${i1 / dirRatio}/" else ""
        if (i1 > dirRatio && i1 % dirRatio == 1) {
          zos.putNextEntry(new ZipEntry(dirName)) // New directory
          zos.closeEntry()
        }
        val entryName = s"${dirName}file_$i1"
        if (i1 % 2 != 0) {
          zos.putNextEntry(new ZipEntry(entryName))
          zos.write(sampleFile(sizePerFile))
        } else { // nested zip
          zos.putNextEntry(new ZipEntry(s"$entryName.zip"))
          val bais = sampleZipFile(1, sizePerFile)
          val arr = new Array[Byte](bais.available)
          bais.read(arr)
          zos.write(arr)
          bais.close()
        }
        zos.closeEntry()
      })
    }
  }

  private def withZos(f: ZipOutputStream => Unit) = {
    val baos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(baos)
    try {
      f(zos)
      zos.finish()
      new ByteArrayInputStream(baos.toByteArray)
    } finally {
      if (zos != null)
        zos.close()
    }
  }

  private def sampleFile(size: Int) = loremIpsum.take(size).toArray

  private def loremIpsum: Stream[Byte] =
    Stream.concat(
      """|Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent auctor imperdiet
         |velit, eu dapibus nisl dapibus vitae. Sed quam lacus, fringilla posuere ligula at,
         |aliquet laoreet nulla. Aliquam id fermentum justo. Aliquam et massa consequat,
         |pellentesque dolor nec, gravida libero. Phasellus elit eros, finibus eget
         |sollicitudin ac, consectetur sed ante. Etiam ornare lacus blandit nisi gravida
         |accumsan. Sed in lorem arcu. Vivamus et eleifend ligula. Maecenas ut commodo ante.
         |Suspendisse sit amet placerat arcu, porttitor sagittis velit. Quisque gravida mi a
         |porttitor ornare. Cras lorem nisl, sollicitudin vitae odio at, vehicula maximus
         |mauris. Sed ac purus ac turpis pellentesque cursus ac eget est. Pellentesque
         |habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.
         |""".stripMargin.toCharArray.map(_.toByte)
    ) #::: loremIpsum

}
