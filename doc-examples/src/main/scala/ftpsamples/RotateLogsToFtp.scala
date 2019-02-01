/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package ftpsamples

import java.net.InetAddress
import java.nio.file.{Path, Paths}

import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.alpakka.ftp.{FtpCredentials, SftpIdentity, SftpSettings}
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.scaladsl.{Compression, Flow, Keep, Source}
import akka.util.ByteString
import playground.ActorSystemAvailable

import scala.util.{Failure, Success}

object RotateLogsToFtp extends ActorSystemAvailable with App {

  val data = ('a' to 'd').flatMap(letter => Seq.fill(10)(ByteString(letter.toString * 100)))
  val rotator = () => {
    var last: Char = ' '
    (bs: ByteString) =>
      {
        bs.head.toChar match {
          case char if char != last =>
            last = char
            Some(Paths.get(s"log-$char.z"))
          case _ => None
        }
      }
  }

  val identity = SftpIdentity.createFileSftpIdentity("<path_to_identity_file>")
  val credentials = FtpCredentials.create("username", "")
  val settings = SftpSettings(InetAddress.getByName("hostname"))
    .withSftpIdentity(identity)
    .withStrictHostKeyChecking(false)
    .withCredentials(credentials)
  val sink = (path: Path) =>
    Flow[ByteString]
      .via(Compression.gzip)
      .toMat(Sftp.toPath(s"tmp/${path.getFileName.toString}", settings))(Keep.right)

  val completion = Source(data).runWith(LogRotatorSink.withSinkFactory(rotator, sink))
  completion.onComplete {
    case Success(_) => println("Done")
    case Failure(f) => println(f.getMessage)
  }
}
