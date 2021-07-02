/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.duration._

import java.nio.file.Paths

class TarArchiverSpec extends TestKit(ActorSystem("ziparchive")) with AnyWordSpecLike with ScalaFutures {

  "yadi" should {
    "yadi" in {
      import system.dispatcher

      Directory
        .walk(Paths.get("/Users/johan/Downloads/2020-01-24-16-07-16/"))
        .filter(_.getFileName.toString.endsWith(".tgz"))
        .flatMapConcat { p =>
          println(s"Path: $p")
          FileIO.fromPath(p)
        }
        .via(Compression.gunzip())
        .via(Archive.tarReader())
        .mapAsync(1) {
          case (metadata, source) =>
            println(s"Metadata entry: $metadata")
            /* if (metadata.isDirectory) {
              println(s"Directory")
              source.run()
            } else { */
            source.runFold(ByteString.empty)(_ ++ _).map(bs => bs.size) /*.map { bs: ByteString =>
            println(s"File")
            metadata -> bs
          } */

        }
        .runWith(Sink.seq)
        .futureValue(timeout(1.minute)) // should complete

    }
  }

}
