/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.backblazeb2

import akka.stream.alpakka.backblazeb2.Protocol.B2Response

import scala.concurrent.Await
import scala.concurrent.duration._

package object scaladsl {
  val timeout = 10.seconds

  def extractFromResponse[T](response: B2Response[T]): T = {
    val result = Await.result(response, timeout)
    result match {
      case Left(x) =>
        sys.error(s"Failed to obtain: $x")

      case Right(x) =>
        x
    }
  }
}
