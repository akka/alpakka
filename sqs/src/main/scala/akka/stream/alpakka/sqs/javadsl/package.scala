/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs

import akka.japi.function.Function

package object javadsl {
  def identityFunction[T] = new Function[T, T] {
    override def apply(param: T): T = param
  }
}
