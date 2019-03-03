/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.strategy

import akka.annotation.InternalApi

/**
 * Internal API
 */
@InternalApi
private[hdfs] trait Strategy {
  type S <: Strategy
  def should(): Boolean
  def reset(): S
  def update(offset: Long): S
}
