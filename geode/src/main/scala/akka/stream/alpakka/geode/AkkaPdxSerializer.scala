/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.geode

import org.apache.geode.pdx.PdxSerializer

/**
 * Base interface for Geode `PdxSerializer`s in Alpakka Geode.
 */
trait AkkaPdxSerializer[V] extends PdxSerializer {
  def clazz: Class[V]
}
