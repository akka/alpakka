/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import io.pravega.client.stream.{EventPointer, Position}

class PravegaEvent[+Message](val message: Message, val position: Position, val eventPointer: EventPointer)
