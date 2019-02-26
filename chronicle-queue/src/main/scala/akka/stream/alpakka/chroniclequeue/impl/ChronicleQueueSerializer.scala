/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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

import net.openhft.chronicle.wire.{WireIn, WireOut}

import net.openhft.chronicle.wire.AbstractMarshallable

trait ChronicleQueueSerializer[T] extends AbstractMarshallable {

  /**
   * Reads an element from the wire.
   *
   * @param wire The wire.
   * @return The element as an option, or None if no element is available.
   */
  def readElement(wire: WireIn): Option[T]

  /**
   * Writes an element to the wire.
   *
   * @param element The element to be written.
   * @param wire The wire.
   */
  def writeElement(element: T, wire: WireOut): Unit
}
