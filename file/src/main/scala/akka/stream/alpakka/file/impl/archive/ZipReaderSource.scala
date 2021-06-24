/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.util.ByteString

import java.io.{File, FileInputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

case class ZipArchiveMetadata(name: String)

@InternalApi class ZipEntrySource(n: ZipArchiveMetadata, f: File, chunkSize: Int)
    extends GraphStage[SourceShape[ByteString]] {
  private val out = Outlet[ByteString]("flowOut")
  override val shape: SourceShape[ByteString] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val zis = new ZipInputStream(new FileInputStream(f))
      var entry: ZipEntry = null
      val data = new Array[Byte](chunkSize)

      def seek() = {
        while ({
          entry = zis.getNextEntry()
          entry != null && entry.getName != n.name
        }) {
          zis.closeEntry()
        }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (entry == null) {
              seek()
              if (entry == null) {
                failStage(new Exception("After a seek the part is not found"))
              }
            }

            val c = zis.read(data, 0, chunkSize)
            if (c == -1) {
              completeStage()
            } else {
              push(out, ByteString.fromArray(data, 0, c))
            }
          }
        }
      )

      override def postStop(): Unit = {
        super.postStop()
        zis.close()
      }
    }
}

@InternalApi class ZipSource(f: File, chunkSize: Int)
    extends GraphStage[SourceShape[(ZipArchiveMetadata, Source[ByteString, NotUsed])]] {
  private val out = Outlet[(ZipArchiveMetadata, Source[ByteString, NotUsed])]("flowOut")
  override val shape: SourceShape[(ZipArchiveMetadata, Source[ByteString, NotUsed])] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val zis = new ZipInputStream(new FileInputStream(f))

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            val e = zis.getNextEntry
            if (e != null) {
              val n = ZipArchiveMetadata(e.getName)
              zis.closeEntry()
              push(out, n -> Source.fromGraph(new ZipEntrySource(n, f, chunkSize)))
            } else {
              zis.close()
              completeStage()
            }
          }
        }
      )

      override def postStop(): Unit = {
        super.postStop()
        zis.close()
      }
    }
}
