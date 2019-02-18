/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

// ORIGINAL LICENCE
/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.stream.alpakka.chroniclequeue.impl

import java.io.{File, FileNotFoundException}

import scala.compat.java8.FunctionConverters._

import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import net.openhft.chronicle.bytes.MappedBytesStore
import net.openhft.chronicle.core.OS
import net.openhft.chronicle.queue.impl.single.{SingleChronicleQueue, SingleChronicleQueueBuilder}
import net.openhft.chronicle.queue.impl.{RollingResourcesCache, StoreFileListener}
import net.openhft.chronicle.wire.{ReadMarshallable, WireIn, WireOut, WriteMarshallable}

import akka.stream.alpakka.chroniclequeue.scaladsl

case class Event[T](outputPortId: Int, index: Long, entry: T)

/**
 * Persistent queue using Chronicle Queue as implementation.
 *
 * @tparam T The type of elements to be stored in the queue.
 */
class PersistentQueue[T](config: scaladsl.ChronicleQueueConfig,
                         onCommitCallback: Int => Unit = _ => {},
                         indexName: String = "tailer.idx")(
    implicit val serializer: ChronicleQueueSerializer[T]
) {

  def this(config: Config)(implicit serializer: ChronicleQueueSerializer[T]) =
    this(scaladsl.ChronicleQueueConfig.from(config))

  def this(config: Config, indexName: String)(implicit serializer: ChronicleQueueSerializer[T]) =
    this(scaladsl.ChronicleQueueConfig.from(config), indexName = indexName)

  def this(persistDir: File)(implicit serializer: ChronicleQueueSerializer[T]) =
    this(scaladsl.ChronicleQueueConfig(persistDir))

  def this(persistDir: File, indexName: String)(implicit serializer: ChronicleQueueSerializer[T]) =
    this(scaladsl.ChronicleQueueConfig(persistDir), indexName = indexName)

  def withOnCommitCallback(onCommitCallback: Int => Unit) = new PersistentQueue[T](config, onCommitCallback)

  private val logger = LoggerFactory.getLogger(classOf[PersistentQueue[T]])

  import config._
  if (!persistDir.isDirectory && !persistDir.mkdirs())
    throw new FileNotFoundException(persistDir.getAbsolutePath)

  private[stream] val resourceManager = new ResourceManager

  private val builder = SingleChronicleQueueBuilder
    .single(persistDir.getAbsolutePath)
    .wireType(wireType)
    .rollCycle(rollCycle)
    .blockSize(blockSize.toInt)
    .indexSpacing(indexSpacing)
    .indexCount(indexCount)
    .storeFileListener(resourceManager)

  private val queue = builder.build()

  private val appender = queue.acquireAppender
  private val reader = Vector.tabulate(outputPorts)(_ => queue.createTailer)

  private val Tailer = indexName
  private val path =
    if (Tailer != null) new File(persistDir, Tailer)
    else null.asInstanceOf[File]

  private var indexMounted = false
  private var indexFile: IndexFile = _
  private var indexStore: MappedBytesStore = _
  private var closed = false
  private val cycle = Array.ofDim[Int](outputPorts)
  private val lastCommitIndex = Array.ofDim[Long](outputPorts)

  val totalOutputPorts: Int = outputPorts

  import SingleChronicleQueue.SUFFIX

  // `fileIdParser` will parse a given filename to its long value.
  // The value is based on epoch time and grows incrementally.
  // https://github.com/OpenHFT/Chronicle-Queue/blob/chronicle-queue-4.16.5/src/main/java/net/openhft/chronicle/queue/RollCycles.java#L85
  private[stream] val fileIdParser = new RollingResourcesCache(
    queue.rollCycle(),
    queue.epoch(),
    ((name: String) => new File(builder.path(), name + SUFFIX)).asJava,
    ((file: File) => file.getName.stripSuffix(SUFFIX)).asJava
  )

  private def mountIndexFile(): Unit =
    if (Tailer != null) {
      indexFile = IndexFile.of(path, OS.pageSize())
      indexStore = indexFile.acquireByteStore(0L)
      indexMounted = true
    }

  if (Tailer != null && path.isFile) {
    mountIndexFile()
    (0 until outputPorts).foreach { outputPortId =>
      val startIdx = read(outputPortId)
      logger.info(s"Setting idx for outputPort ${outputPortId.toString} - ${startIdx.toString}")
      reader(outputPortId).moveToIndex(startIdx)
      dequeue(outputPortId) // dequeue the first read element
      lastCommitIndex(outputPortId) = startIdx
      cycle(outputPortId) = queue.rollCycle().toCycle(startIdx)
    }
  }

  /**
   * Adds an element to the queue.
   *
   * @param element The element to be added.
   */
  def enqueue(element: T): Unit =
    appender.writeDocument(new WriteMarshallable {
      override def writeMarshallable(wire: WireOut): Unit = serializer.writeElement(element, wire)
    })

  /**
   * Fetches the first element from the queue and removes it from the queue.
   *
   * @return The first element in the queue, or None if the queue is empty.
   */
  def dequeue(outputPortId: Int = 0): Option[Event[T]] = {
    var output: Option[Event[T]] = None
    if (reader(outputPortId).readDocument(new ReadMarshallable {
          override def readMarshallable(wire: WireIn): Unit =
            output = {
              val element = serializer.readElement(wire)
              val index = reader(outputPortId).index
              element.map { e =>
                Event[T](outputPortId, index, e)
              }
            }
        })) output
    else None
  }

  /**
   * Commits the queue's index, this index is mounted when
   * the queue is initialized next time
   *
   * @param outputPortId The id of the output port
   * @param index to be committed for next read
   */
  def commit(outputPortId: Int, index: Long, verify: Boolean = true): Unit = {
    if (verify) verifyCommitOrder(outputPortId, index)
    if (!indexMounted) mountIndexFile()
    if (Tailer != null) indexStore.writeLong(outputPortId << 3, index)
    onCommitCallback(outputPortId)
  }

  protected[stream] def verifyCommitOrder(outputPortId: Int, index: Long): Unit = {
    val lastCommit = lastCommitIndex(outputPortId)
    if (index == lastCommit + 1) lastCommitIndex(outputPortId) = index
    else {
      val newCycle = queue.rollCycle().toCycle(index)
      if (newCycle == cycle(outputPortId) + 1 || lastCommit == 0) {
        cycle(outputPortId) = newCycle
        lastCommitIndex(outputPortId) = index
      } else {
        config.commitOrderPolicy match {
          case scaladsl.Lenient =>
            logger.info(
              s"Missing or out of order commits.  previous: ${lastCommitIndex(outputPortId)} latest: $index cycle: ${cycle(outputPortId)}"
            )
            lastCommitIndex(outputPortId) = index
          case scaladsl.Strict =>
            val msg =
              s"Missing or out of order commits.  previous: ${lastCommitIndex(outputPortId)} latest: $index cycle: ${cycle(outputPortId)}"
            logger.error(msg)
            // Not closing the queue here as `Supervision.Decider` might resume the stream.
            throw new CommitOrderException(msg, lastCommitIndex(outputPortId), index, cycle(outputPortId))
        }
      }
    }
  }

  // Reads the given outputPort's queue index
  private[stream] def read(outputPortId: Int): Long = {
    if (!indexMounted) mountIndexFile()
    if (Tailer != null) indexStore.readLong(outputPortId << 3)
    else 0L
  }

  /**
   * Closes the queue and all its persistent storage.
   */
  def close(): Unit = {
    closed = true
    queue.close()
    if (Tailer != null) {
      Option(indexStore).foreach { store =>
        if (store.refCount > 0) store.release()
      }
      Option(indexFile).foreach { file =>
        file.release()
      }
    }
  }

  private[stream] def isClosed = closed

  /**
   * Removes queue's data files automatically after all
   * outputPorts releases processed queue files.
   * The callback `onReleased` is called once processed.
   */
  class ResourceManager extends StoreFileListener {

    override def onAcquired(cycle: Int, file: File): Unit = {
      logger.info(s"File acquired ${cycle.toString} - ${file.getPath}")
      super.onAcquired(cycle, file)
    }

    override def onReleased(cycle: Int, file: File): Unit =
      if (minCycle >= cycle) deleteOlderFiles(cycle, file)

    private def minCycle = reader.iterator.map(_.cycle).min

    // deletes old files whose long value (based on epoch) is < released file's long value.
    private def deleteOlderFiles(cycle: Int, releasedFile: File): Unit =
      for {
        allFiles <- Option(persistDir.listFiles).toSeq
        file <- allFiles
        if file.getName.endsWith(SUFFIX) && fileIdParser.toLong(releasedFile) > (fileIdParser.toLong(file))
      } {
        logger.info(s"File released ${cycle.toString} - ${file.getPath}")
        if (!file.delete()) {
          logger.error(s"Failed to DELETE ${file.getPath}")
        }
      }
  }
}

class CommitOrderException(message: String, previousIndex: Long, lastIndex: Long, cycle: Int)
    extends RuntimeException(message)
