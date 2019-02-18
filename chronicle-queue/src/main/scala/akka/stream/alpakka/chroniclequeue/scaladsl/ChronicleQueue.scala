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
package akka.stream.alpakka.chroniclequeue.scaladsl

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.config.Config

import akka.stream.alpakka.chroniclequeue.impl._

/**
 * Persists all incoming upstream element to a memory mapped queue before publishing it to downstream consumer.
 *
 * '''Emits when''' one of the inputs has an element available
 *
 * '''Does not Backpressure''' upstream when downstream backpressures, instead buffers the stream element to
 * memory mapped queue
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' downstream cancels
 *
 */
class ChronicleQueue[T](queue: PersistentQueue[T], onPushCallback: () => Unit)(
    implicit serializer: ChronicleQueueSerializer[T],
    system: ActorSystem
) extends ChronicleQueueBase[T, T](queue, onPushCallback)(serializer, system) {

  def this(queue: PersistentQueue[T])(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(queue, () => {})

  def this(config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](config))

  def this(config: Config, indexName: String)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](config, indexName = indexName))

  def this(persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](persistDir))

  def this(persistDir: File, indexName: String)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](persistDir, indexName = indexName))

  def withOnPushCallback(onPushCallback: () => Unit) = new ChronicleQueue[T](queue, onPushCallback)

  def withOnCommitCallback(onCommitCallback: () => Unit) =
    new ChronicleQueue[T](queue.withOnCommitCallback(i => onCommitCallback()), onPushCallback)

  // verify ... should be ok ...
  override protected def autoCommit(index: Long) = queue.commit(defaultOutputPort, index)

  override protected def elementOut(e: Event[T]): T = e.entry
}

object ChronicleQueue {

  def apply[T](config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    new ChronicleQueue[T](config)

  def apply[T](persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    new ChronicleQueue[T](persistDir)

  def apply[T](config: Config, indexName: String)(implicit serializer: ChronicleQueueSerializer[T],
                                                  system: ActorSystem) =
    new ChronicleQueue[T](config, indexName = indexName)

  def apply[T](persistDir: File, indexName: String)(implicit serializer: ChronicleQueueSerializer[T],
                                                    system: ActorSystem) =
    new ChronicleQueue[T](persistDir, indexName = indexName)

  def source[T](persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    Source.fromGraph(new ChronicleQueueSource[T](new PersistentQueue[T](persistDir)))

  def source[T](config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    Source.fromGraph(new ChronicleQueueSource[T](new PersistentQueue[T](config)))

  def source[T](persistDir: File, indexName: String)(implicit serializer: ChronicleQueueSerializer[T],
                                                     system: ActorSystem) =
    Source.fromGraph(new ChronicleQueueSource[T](new PersistentQueue[T](persistDir, indexName = indexName)))

  def source[T](config: Config, indexName: String)(implicit serializer: ChronicleQueueSerializer[T],
                                                   system: ActorSystem) =
    Source.fromGraph(new ChronicleQueueSource[T](new PersistentQueue[T](config, indexName = indexName)))

  def sink[T](persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    Sink.fromGraph(new ChronicleQueueSink[T](new PersistentQueue[T](persistDir, indexName = null)))

  def sink[T](config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    Sink.fromGraph(new ChronicleQueueSink[T](new PersistentQueue[T](config, indexName = null)))

}

/**
 * Persists all incoming upstream element to a memory mapped queue before publishing it to downstream consumer.
 *
 * '''Emits when''' one of the inputs has an element available
 *
 * '''Does not Backpressure''' upstream when downstream backpressures, instead buffers the stream element to
 * memory mapped queue
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' downstream cancels
 *
 * A commit guarantee can be ensured to avoid data lost while consuming stream elements by adding a commit stage
 * after downstream consumer.
 *
 */
class ChronicleQueueAtLeastOnce[T] private (queue: PersistentQueue[T], onPushCallback: () => Unit)(
    implicit serializer: ChronicleQueueSerializer[T],
    system: ActorSystem
) extends ChronicleQueueBase[T, Event[T]](queue, onPushCallback)(serializer, system) {

  def this(queue: PersistentQueue[T])(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(queue, () => {})

  def this(config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](config))

  def this(persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    this(new PersistentQueue[T](persistDir))

  def withOnPushCallback(onPushCallback: () => Unit) = new ChronicleQueueAtLeastOnce[T](queue, onPushCallback)

  def withOnCommitCallback(onCommitCallback: () => Unit) =
    new ChronicleQueueAtLeastOnce[T](queue.withOnCommitCallback(i => onCommitCallback()), onPushCallback)

  override protected def elementOut(e: Event[T]): Event[T] = e

  def commit[U] = Flow[Event[U]].map { element =>
    if (!upstreamFailed) {
      queue.commit(element.outputPortId, element.index)
      if (upstreamFinished) queueCloserActor ! Committed(element.outputPortId, element.index)
    }
    element
  }
}

object ChronicleQueueAtLeastOnce {

  def apply[T](config: Config)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    new ChronicleQueueAtLeastOnce[T](config)

  def apply[T](persistDir: File)(implicit serializer: ChronicleQueueSerializer[T], system: ActorSystem) =
    new ChronicleQueueAtLeastOnce[T](persistDir)
}
