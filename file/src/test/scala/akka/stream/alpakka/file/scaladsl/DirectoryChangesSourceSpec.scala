/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.file.scaladsl

import java.nio.file.FileSystems

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.duration._

object DirectoryChangesSourceSpec {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) throw new IllegalArgumentException("Usage: DirectoryChangesSourceTest [path]")
    val path: String = args(0)

    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer = ActorMaterializer()

    // #minimal-sample
    val fs = FileSystems.getDefault
    val changes = DirectoryChangesSource(fs.getPath(path), pollInterval = 1.second, maxBufferSize = 1000)
    changes.runForeach {
      case (path, change) => println("Path: " + path + ", Change: " + change)
    }
    // #minimal-sample
  }
}
