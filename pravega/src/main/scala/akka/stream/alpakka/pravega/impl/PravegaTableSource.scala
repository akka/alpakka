/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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
import akka.stream.alpakka.pravega.TableSettings
import io.pravega.client.KeyValueTableFactory
import io.pravega.client.tables.{IteratorItem, KeyValueTable, KeyValueTableClientConfiguration, TableEntry}

import scala.collection.mutable

import java.util.concurrent.Semaphore
import io.pravega.common.util.AsyncIterator
@InternalApi private final class PravegaTableSourceStageLogic[K, V](
    shape: SourceShape[(K, V)],
    val scope: String,
    tableName: String,
    keyFamily: String,
    tableSettings: TableSettings[K, V],
    startupPromise: Promise[Done]
) extends GraphStageLogic(shape)
    with StageLogging {

  override protected def logSource = classOf[PravegaTableSourceStageLogic[K, V]]

  private def out = shape.out

  private var keyValueTableFactory: KeyValueTableFactory = _
  private var table: KeyValueTable[K, V] = _

  private val queue = mutable.Queue.empty[(K, V)]

  private val semaphore = new Semaphore(tableSettings.maximumInflightMessages)

  private var closing = false

  val logThat: AsyncCallback[String] = getAsyncCallback { message =>
    log.info(message)
  }

  private def pushElement(out: Outlet[(K, V)], element: (K, V)) = {
    push(out, element)
    semaphore.release()
  }

  val onElement: AsyncCallback[(K, V)] = getAsyncCallback[(K, V)] { element =>
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

  def nextIteration(iterator: AsyncIterator[IteratorItem[TableEntry[K, V]]]): Unit =
    iterator.getNext
      .thenAccept(new Consumer[IteratorItem[TableEntry[K, V]]] {
        override def accept(iteratorItem: IteratorItem[TableEntry[K, V]]): Unit = {
          if (iteratorItem == null) {
            onFinish.invoke(())
          } else {
            iteratorItem.getItems.stream().forEach { tableEntry =>
              semaphore.acquire()
              onElement.invoke((tableEntry.getKey.getKey, tableEntry.getValue))
            }
            nextIteration(iterator)
          }
        }
      })

  override def preStart(): Unit = {
    log.info("Start consuming {} by {} ...", tableName, tableSettings.maximumInflightMessages)
    try {

      val kvtClientConfig = KeyValueTableClientConfiguration.builder().build()
      keyValueTableFactory = KeyValueTableFactory
        .withScope(scope, tableSettings.clientConfig)

      table = keyValueTableFactory
        .forKeyValueTable(tableName, tableSettings.keySerializer, tableSettings.valueSerializer, kvtClientConfig)

      //TODO Applied * 10, but no idea :-/
      val iterator = table.entryIterator(keyFamily, tableSettings.maximumInflightMessages * 10, null)

      nextIteration(iterator)

      startupPromise.success(Done)
    } catch {
      case NonFatal(exception) =>
        log.error(exception.getMessage())
        failStage(exception)
    }
  }

  override def postStop(): Unit = {
    log.info("Stopping reader {}", tableName)
    table.close()
    keyValueTableFactory.close()
  }

}

@InternalApi private[pravega] final class PravegaTableSource[K, V](
    scope: String,
    tableName: String,
    keyFamily: String,
    tableSettings: TableSettings[K, V]
) extends GraphStageWithMaterializedValue[SourceShape[(K, V)], Future[Done]] {

  private val out: Outlet[(K, V)] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: SourceShape[(K, V)] = SourceShape(out)

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
      keyFamily,
      tableSettings,
      startupPromise
    )

    (logic, startupPromise.future)

  }

}
