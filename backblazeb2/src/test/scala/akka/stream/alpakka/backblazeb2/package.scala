/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka

import akka.stream.alpakka.backblazeb2.Protocol.B2Response
import scala.concurrent.duration._
import scala.concurrent.Await

package object backblazeb2 {
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
