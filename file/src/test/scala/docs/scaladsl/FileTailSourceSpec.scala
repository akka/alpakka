/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl
import java.nio.file.FileSystems

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.duration._

object FileTailSourceSpec {

  // small sample of usage, tails the first argument file path
  def main(args: Array[String]): Unit = {
    if (args.length != 1) throw new IllegalArgumentException("Usage: FileTailSourceTest [path]")
    val path: String = args(0)

    implicit val system: ActorSystem = ActorSystem()

    // #simple-lines
    import akka.stream.alpakka.file.scaladsl.FileTailSource

    val fs = FileSystems.getDefault
    val lines: Source[String, NotUsed] = FileTailSource.lines(
      path = fs.getPath(path),
      maxLineSize = 8192,
      pollingInterval = 250.millis
    )

    lines.runForeach(line => System.out.println(line))
    // #simple-lines
  }

}
