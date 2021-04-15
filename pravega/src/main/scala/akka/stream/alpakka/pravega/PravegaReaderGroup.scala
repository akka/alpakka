/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import io.pravega.client.stream.ReaderGroup

case class PravegaReaderGroup(readerGroup: ReaderGroup, steamsName: Set[String]) {
  def getScope: String = readerGroup.getScope

}
