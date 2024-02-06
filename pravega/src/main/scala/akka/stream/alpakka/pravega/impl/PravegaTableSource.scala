/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import java.util.function.Consumer
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import akka.stream.ActorAttributes

import akka.stream.alpakka.pravega.TableReaderSettings
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.{
  IteratorItem,
  KeyValueTable,
  KeyValueTableClientConfiguration,
  TableEntry => JTableEntry
}

import akka.stream.alpakka.pravega.TableEntry

import scala.collection.mutable
import java.util.concurrent.Semaphore
import io.pravega.common.util.AsyncIterator
@InternalApi private final class PravegaTableSourceStageLogic[K, V](
    shape: SourceShape[TableEntry[V]],
    val scope: String,
    tableName: String,
    tableReaderSettings: TableReaderSettings[K, V],
    startupPromise: Promise[Done]
) extends GraphStageLogic(shape)
    with StageLogging {

  override protected def logSource = classOf[PravegaTableSourceStageLogic[K, V]]

  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _
  private var table: KeyValueTable = _

  private val queue = mutable.Queue.empty[TableEntry[V]]

  private val semaphore = new Semaphore(tableReaderSettings.maximumInflightMessages)

  private var closing = false

  val logThat: AsyncCallback[String] = getAsyncCallback { message =>
    log.info(message)
  }

  private def pushElement(out: Outlet[TableEntry[V]], element: TableEntry[V]) = {
    push(out, element)
    semaphore.release()
  }

  val onElement: AsyncCallback[TableEntry[V]] = getAsyncCallback[TableEntry[V]] { element =>
    if (isAvailable(out) && queue.isEmpty)
      pushElement(out, element)
    else
      queue.enqueue(element)

  }

  val onFinish: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
    closing = true
    if (queue.isEmpty)
      completeStage()

  }

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = {
        if (!queue.isEmpty)
          pushElement(out, queue.dequeue())
        if (closing && queue.isEmpty)
          completeStage()
      }
    }
  )

  def nextIteration(iterator: AsyncIterator[IteratorItem[JTableEntry]]): Unit =
    iterator.getNext
      .thenAccept(new Consumer[IteratorItem[JTableEntry]] {
        override def accept(iteratorItem: IteratorItem[JTableEntry]): Unit = {
          if (iteratorItem == null) {
            onFinish.invoke(())
          } else {
            iteratorItem.getItems.stream().forEach { tableEntry =>
              semaphore.acquire()

              val entry = new TableEntry(tableEntry.getKey(),
                                         tableEntry.getVersion(),
                                         tableReaderSettings.valueSerializer.deserialize(tableEntry.getValue())
              )
              onElement.invoke(entry)
            }
            nextIteration(iterator)
          }
        }
      })

  override def preStart(): Unit = {
    log.debug("Start consuming {} by {} ...", tableName, tableReaderSettings.maximumInflightMessages)
    try {

      val kvtClientConfig = KeyValueTableClientConfiguration.builder().build()
      keyValueTableFactory = KeyValueTableFactory
        .withScope(scope, tableReaderSettings.clientConfig)

      table = keyValueTableFactory
        .forKeyValueTable(tableName, kvtClientConfig)

      val iterator = table.iterator().maxIterationSize(tableReaderSettings.maxEntriesAtOnce).all().entries()

      nextIteration(iterator)

      startupPromise.success(Done)
    } catch {
      case NonFatal(exception) =>
        log.error(exception.getMessage())
        failStage(exception)
    }
  }

  override def postStop(): Unit = {
    log.debug("Stopping reader {}", tableName)
    table.close()
    keyValueTableFactory.close()
  }

}

@InternalApi private[pravega] final class PravegaTableSource[K, V](
    scope: String,
    tableName: String,
    tableReaderSettings: TableReaderSettings[K, V]
) extends GraphStageWithMaterializedValue[SourceShape[TableEntry[V]], Future[Done]] {

  private val out: Outlet[TableEntry[V]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: SourceShape[TableEntry[V]] = SourceShape(out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val startupPromise = Promise[Done]()

    val logic = new PravegaTableSourceStageLogic[K, V](
      shape,
      scope,
      tableName,
      tableReaderSettings,
      startupPromise
    )

    (logic, startupPromise.future)

  }

}
