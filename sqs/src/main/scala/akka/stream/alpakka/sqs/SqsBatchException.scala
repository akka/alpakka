/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

final class SqsBatchException(val batchSize: Int, cause: Exception) extends Exception(cause) {
  def getBatchSize: Int = batchSize
}
