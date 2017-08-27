/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode

import org.apache.geode.pdx.PdxSerializer

trait AkkaPdxSerializer[V] extends PdxSerializer {
  def clazz: Class[V]
}
